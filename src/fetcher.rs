use reqwest::Client;
use serde_json::Value;
use anyhow::{Result, Context};
use crate::models::{Protocol, UnifiedPool, GraphResponse, V3Data, V2Data, V4Data};
use crate::db::Db;
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn, error};
use tokio::time::{sleep, Duration};

const BATCH_SIZE: usize = 1000;
const TVL_THRESHOLD: f64 = 5000.0;
const MAX_RETRIES: u32 = 5; 

pub async fn fetch_and_save(
    client: &Client, 
    db: &mut Db,
    url: &str, 
    protocol: Protocol,
    start_id: Option<String>
) -> Result<()> {
    let mut last_id = start_id.unwrap_or_default();
    let mut has_more = true;
    let mut total_saved = 0;
    let mut page_count = 0;

    info!("🚀 开始任务 [{:?}]", protocol);
    info!("🔗 目标 URL: {}", url);
    info!("📂 起始 ID: \"{}\"", last_id);

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] 页数:{bar} | 已存: {pos} | 最新ID: {msg}").unwrap());

    while has_more {
        page_count += 1;
        let query = build_query(&protocol, &last_id);
        
        let mut attempts = 0;
        let resp_body: Value = loop {
            attempts += 1;
            let result = client.post(url)
                .json(&serde_json::json!({ "query": query }))
                .send()
                .await;

            match result {
                Ok(resp) => {
                    match resp.json::<Value>().await {
                        Ok(body) => break body,
                        Err(e) => {
                            if attempts >= MAX_RETRIES {
                                error!("❌ 解析 JSON 失败 (重试耗尽): {:?}", e);
                                return Err(e.into());
                            }
                            warn!("⚠️ 解析 JSON 失败 (第 {} 次重试): {:?}", attempts, e);
                            sleep(Duration::from_secs(2u64.pow(attempts))).await;
                        }
                    }
                },
                Err(e) => {
                    if attempts >= MAX_RETRIES {
                        error!("❌ 请求失败 (重试耗尽): {:?}", e);
                        return Err(e.into());
                    }
                    warn!("⚠️ 网络请求失败 (第 {} 次重试): {:?}。等待重试...", attempts, e);
                    sleep(Duration::from_secs(2u64.pow(attempts))).await;
                }
            }
        };

        // --- 核心修复：检查 errors 和 data ---
        
        // 1. 如果有 errors，先打印出来，让我们知道发生了什么
        if let Some(errs) = resp_body.get("errors") {
            // 有些 subgraph 会同时返回 data 和 errors（部分成功），所以这里只报 warn
            warn!("⚠️ GraphQL 返回错误信息: {:?}", errs);
        }

        // 2. 严格检查 data 是否存在
        // 如果 data 是 null 或者不存在，说明查询完全失败（可能是 query 写错，或者服务器内部错误）
        if resp_body.get("data").is_none() || resp_body["data"].is_null() {
            error!("❌ 致命错误：服务器响应缺少 'data' 字段！");
            error!("📄 完整响应内容: {:?}", resp_body); // 打印完整内容以便调试
            return Err(anyhow::anyhow!("GraphQL Error: Missing data field"));
        }

        // --- 检查结束，开始解析 ---

        let (mut batch_pools, fetched_count, new_last_id) = parse_response(&protocol, resp_body)?;

        if fetched_count > 0 {
            last_id = new_last_id.clone();
            
            // 粗筛选
            batch_pools.retain(|p| p.tvl_usd >= TVL_THRESHOLD);
            
            if !batch_pools.is_empty() {
                if let Err(e) = db.insert_batch(&batch_pools) {
                    error!("❌ 数据库写入失败: {:?}", e);
                    return Err(e.into());
                }
                total_saved += batch_pools.len();
                pb.inc(batch_pools.len() as u64);
            }
            pb.set_message(last_id.clone());
            
            if page_count % 10 == 0 {
                info!("第 {} 页完成 | 当前 ID: {} | 已存总数: {}", page_count, last_id, total_saved);
            }
        } else {
            has_more = false;
        }

        if fetched_count < BATCH_SIZE {
            has_more = false;
        }

        sleep(Duration::from_millis(200)).await;
    }

    pb.finish_with_message(format!("任务完成，共存入 {} 条数据", total_saved));
    info!("✅ [{:?}] 拉取结束，有效数据: {}", protocol, total_saved);
    Ok(())
}

fn parse_response(protocol: &Protocol, body: Value) -> Result<(Vec<UnifiedPool>, usize, String)> {
    let mut pools = Vec::new();
    let mut last_id = String::new();
    let count;

    match protocol {
        Protocol::UniV3 | Protocol::AerodromeV3 => {
            let data: GraphResponse<V3Data> = serde_json::from_value(body).context("解析 V3 数据结构失败")?;
            count = data.data.pools.len();
            if let Some(last) = data.data.pools.last() {
                last_id = last.id.clone();
            }
            for p in data.data.pools {
                let tvl = p.totalValueLockedUSD.parse::<f64>().unwrap_or(0.0);
                let fee = p.feeTier.parse::<u32>().unwrap_or(0);
                let raw = serde_json::to_string(&p).unwrap();
                let extra = serde_json::json!({
                    "liquidity": p.liquidity,
                    "tvl_usd": p.totalValueLockedUSD
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol,
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol,
                    fee,
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        },
        Protocol::UniV2 => {
            let data: GraphResponse<V2Data> = serde_json::from_value(body).context("解析 V2 数据结构失败")?;
            count = data.data.pairs.len();
            if let Some(last) = data.data.pairs.last() {
                last_id = last.id.clone();
            }
            for p in data.data.pairs {
                let tvl = p.reserveUSD.parse::<f64>().unwrap_or(0.0);
                let raw = serde_json::to_string(&p).unwrap();
                let extra = serde_json::json!({
                    "reserveUSD": p.reserveUSD
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol,
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol,
                    fee: 3000,
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        },
        Protocol::UniV4 => {
            let data: GraphResponse<V4Data> = serde_json::from_value(body).context("解析 V4 数据结构失败")?;
            count = data.data.pools.len();
            if let Some(last) = data.data.pools.last() {
                last_id = last.id.clone();
            }
            for p in data.data.pools {
                let tvl = p.totalValueLockedUSD.parse::<f64>().unwrap_or(0.0);
                let fee = p.feeTier.parse::<u32>().unwrap_or(0);
                let raw = serde_json::to_string(&p).unwrap();
                // V4 特有：将 hooks 放入 extra_data
                let extra = serde_json::json!({
                    "liquidity": p.liquidity,
                    "tvl_usd": p.totalValueLockedUSD,
                    "hooks": p.hooks 
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol,
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol,
                    fee,
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        }
    }
    Ok((pools, count, last_id))
}

fn build_query(protocol: &Protocol, last_id: &str) -> String {
    match protocol {
        Protocol::UniV3 | Protocol::AerodromeV3 => format!(
            r#"{{
                pools(first: {}, where: {{ id_gt: "{}" }}, orderBy: id, orderDirection: asc) {{
                    id
                    feeTier
                    liquidity
                    totalValueLockedUSD
                    token0 {{ id symbol }}
                    token1 {{ id symbol }}
                }}
            }}"#,
            BATCH_SIZE, last_id
        ),
        Protocol::UniV2 => format!(
            r#"{{
                pairs(first: {}, where: {{ id_gt: "{}" }}, orderBy: id, orderDirection: asc) {{
                    id
                    reserveUSD
                    token0 {{ id symbol }}
                    token1 {{ id symbol }}
                }}
            }}"#,
            BATCH_SIZE, last_id
        ),
        Protocol::UniV4 => format!(
            r#"{{
                pools(first: {}, where: {{ id_gt: "{}" }}, orderBy: id, orderDirection: asc) {{
                    id
                    feeTier
                    hooks
                    liquidity
                    totalValueLockedUSD
                    token0 {{ id symbol }}
                    token1 {{ id symbol }}
                }}
            }}"#,
            BATCH_SIZE, last_id
        ),
    }
}