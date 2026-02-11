use reqwest::Client;
use serde_json::Value;
use anyhow::{Result, Context};
use crate::models::{Protocol, UnifiedPool, GraphResponse, V3Data, V2Data};
use crate::db::Db;
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn, error};
use tokio::time::{sleep, Duration};

const BATCH_SIZE: usize = 1000;
const TVL_THRESHOLD: f64 = 5000.0; // 过滤阈值：5000 USD
const MAX_RETRIES: u32 = 5;        // 最大重试次数

pub async fn fetch_and_save(
    client: &Client, 
    db: &mut Db,
    url: &str, 
    protocol: Protocol
) -> Result<()> {
    let mut last_id = "".to_string();
    let mut has_more = true;
    let mut total_saved = 0;

    info!("开始任务 [{:?}] | 目标 URL: {}", protocol, url);

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] 已存入: {pos} | 当前ID: {msg}").unwrap());

    while has_more {
        let query = build_query(&protocol, &last_id);
        
        // --- 重试逻辑开始 ---
        let mut attempts = 0;
        let resp_body: Value = loop {
            attempts += 1;
            
            // 发送请求
            let result = client.post(url)
                .json(&serde_json::json!({ "query": query }))
                .send()
                .await;

            match result {
                Ok(resp) => {
                    // 尝试解析响应为 JSON
                    match resp.json::<Value>().await {
                        Ok(body) => break body, // 成功拿到 JSON，跳出重试循环
                        Err(e) => {
                            if attempts >= MAX_RETRIES {
                                error!("解析 JSON 失败 (重试耗尽): {:?}", e);
                                return Err(e.into());
                            }
                            warn!("解析 JSON 失败 (第 {} 次重试): {:?}", attempts, e);
                        }
                    }
                },
                Err(e) => {
                    if attempts >= MAX_RETRIES {
                        error!("请求失败 (重试耗尽): {:?}", e);
                        return Err(e.into());
                    }
                    warn!("网络请求失败 (第 {} 次重试): {:?}。等待重试...", attempts, e);
                    // 指数退避：第一次等 2s，第二次 4s，第三次 8s...
                    sleep(Duration::from_secs(2u64.pow(attempts))).await;
                }
            }
        };
        // --- 重试逻辑结束 ---

        // 检查 GraphQL 错误
        if let Some(errs) = resp_body.get("errors") {
            warn!("GraphQL 返回错误: {:?}", errs);
            // 遇到错误可以选择跳过或停止，这里选择记录警告并继续尝试解析数据（如果有的话）
        }

        // 解析数据
        let (mut batch_pools, fetched_count, new_last_id) = parse_response(&protocol, resp_body)?;

        if fetched_count > 0 {
            last_id = new_last_id;
            
            // 1. 粗筛选：保留 TVL >= 5000 的池子
            batch_pools.retain(|p| p.tvl_usd >= TVL_THRESHOLD);
            
            // 2. 立即写入数据库
            if !batch_pools.is_empty() {
                if let Err(e) = db.insert_batch(&batch_pools) {
                    error!("数据库写入失败: {:?}", e);
                    return Err(e.into());
                }
                total_saved += batch_pools.len();
                pb.inc(batch_pools.len() as u64);
            }
            pb.set_message(last_id.clone());
        }

        // 如果拉取到的数量少于每页最大数量，说明是最后一页
        if fetched_count < BATCH_SIZE {
            has_more = false;
        }

        // --- 主动降速 ---
        // 每次成功请求后暂停 100ms，避免触发 API 速率限制
        sleep(Duration::from_millis(100)).await;
    }

    pb.finish_with_message(format!("任务完成，共存入 {} 条数据", total_saved));
    info!("[{:?}] 拉取结束，有效数据: {}", protocol, total_saved);
    Ok(())
}

fn parse_response(protocol: &Protocol, body: Value) -> Result<(Vec<UnifiedPool>, usize, String)> {
    let mut pools = Vec::new();
    let mut last_id = String::new();
    let count;

    match protocol {
        Protocol::UniV3 | Protocol::AerodromeV3 => {
            let data: GraphResponse<V3Data> = serde_json::from_value(body).context("解析 V3 数据失败")?;
            count = data.data.pools.len();
            if let Some(last) = data.data.pools.last() {
                last_id = last.id.clone();
            }
            for p in data.data.pools {
                let tvl = p.totalValueLockedUSD.parse::<f64>().unwrap_or(0.0);
                let fee = p.feeTier.parse::<u32>().unwrap_or(0);
                
                // 备份原始 JSON
                let raw = serde_json::to_string(&p).unwrap();
                
                // 构造 extra_data，保留流动性等信息
                let extra = serde_json::json!({
                    "liquidity": p.liquidity,
                    "tvl_usd": p.totalValueLockedUSD,
                    // 虽然有了独立字段，但在 JSON 里也保留一份备份
                    "symbol0": p.token0.symbol,
                    "symbol1": p.token1.symbol
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol, // 确保解析了 Symbol
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol, // 确保解析了 Symbol
                    fee,
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        },
        Protocol::UniV2 => {
            let data: GraphResponse<V2Data> = serde_json::from_value(body).context("解析 V2 数据失败")?;
            count = data.data.pairs.len();
            if let Some(last) = data.data.pairs.last() {
                last_id = last.id.clone();
            }
            for p in data.data.pairs {
                let tvl = p.reserveUSD.parse::<f64>().unwrap_or(0.0);
                let raw = serde_json::to_string(&p).unwrap();
                let extra = serde_json::json!({
                    "reserveUSD": p.reserveUSD,
                    "symbol0": p.token0.symbol,
                    "symbol1": p.token1.symbol
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol, // 确保解析了 Symbol
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol, // 确保解析了 Symbol
                    fee: 3000, // V2 默认 0.3%
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
    }
}