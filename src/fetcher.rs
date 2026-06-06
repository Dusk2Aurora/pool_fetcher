use reqwest::Client;
use serde_json::Value;
use anyhow::{Result, Context};
use crate::models::{Protocol, UnifiedPool, GraphResponse, V3Data, V2Data, V4Data};
use crate::db::Db;
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn, error};
use tokio::time::{sleep, Duration};

const BATCH_SIZE: usize = 1000;
const MAX_POOLS: usize = 5000;
const TVL_THRESHOLD: f64 = 1.0;
const MAX_RETRIES: u32 = 5;

pub async fn fetch_and_save(
    client: &Client,
    db: &mut Db,
    url: &str,
    protocol: Protocol,
) -> Result<()> {
    let mut total_saved = 0;
    let mut skip: usize = 0;
    let mut page_count = 0;

    info!("🚀 开始任务 [{:?}]，拉取上限: {} 条 (按 TVL 降序)", protocol, MAX_POOLS);
    info!("🔗 目标 URL: {}", url);

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] 页数:{bar} | 已存: {pos} | Skip: {msg}").unwrap());

    loop {
        if total_saved >= MAX_POOLS {
            info!("✅ 已达到拉取上限 {} 条，停止拉取", MAX_POOLS);
            break;
        }
        
        page_count += 1;
        let query = build_query(&protocol, skip, BATCH_SIZE);
        
        let mut attempts = 0;
        // 用于跟踪 GraphQL 层重试（独立于 HTTP 层）
        let mut graphql_retries = 0;
        let resp_body: Value = loop {
            attempts += 1;
            let result = client.post(url)
                .json(&serde_json::json!({ "query": query }))
                .send()
                .await;

            match result {
                Ok(resp) => {
                    match resp.json::<Value>().await {
                        Ok(body) => {
                            // --- 检查 GraphQL 层 errors 和 data ---
                            let has_data = body.get("data").is_some() && !body["data"].is_null();
                            let has_errors = body.get("errors").is_some();

                            if !has_data {
                                if has_errors {
                                    // 有 errors 但无 data：去中心化网关索引节点不稳定（如 PancakeV4 "bad indexers"）
                                    graphql_retries += 1;
                                    if graphql_retries <= MAX_RETRIES {
                                        warn!(
                                            "⚠️ GraphQL 返回错误且无数据 (GraphQL 重试 {}/{}, HTTP 尝试 {}): {:?}",
                                            graphql_retries, MAX_RETRIES, attempts,
                                            body.get("errors")
                                        );
                                        sleep(Duration::from_secs(2u64.pow(graphql_retries))).await;
                                        continue; // 重试内层循环
                                    } else {
                                        error!(
                                            "❌ GraphQL 持续返回错误且无数据 (已重试 {} 次)，跳过此协议。\n\
                                               👉 这不是代码问题，而是 Subgraph 节点/网关暂时不可用。\n\
                                               错误详情: {:?}",
                                            MAX_RETRIES,
                                            body.get("errors")
                                        );
                                        return Ok(());
                                    }
                                } else {
                                    // 既无 data 也无 errors：真正的异常响应
                                    error!("❌ 致命错误：服务器响应缺少 'data' 字段且无错误信息！");
                                    error!("📄 完整响应内容: {:?}", body);
                                    return Err(anyhow::anyhow!("GraphQL Error: Missing data field"));
                                }
                            }

                            // 如果有 data 但也有 errors，仅打印警告（部分数据可能受损，但继续解析）
                            if has_errors {
                                warn!("⚠️ GraphQL 返回 warnings（data 仍存在，继续解析）: {:?}", body.get("errors"));
                            }

                            break body;
                        },
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

        // --- 检查结束，开始解析 ---

        let (mut batch_pools, fetched_count) = parse_response(&protocol, resp_body)?;

        if fetched_count > 0 {
            // 粗筛选：过滤掉 TVL 过低的池子
            let before_count = batch_pools.len();
            batch_pools.retain(|p| p.tvl_usd >= TVL_THRESHOLD);
            let after_count = batch_pools.len();
            
            if before_count != after_count {
                info!("🔍 [过滤统计] 原始: {}, 经过 TVL 阈值 ({}) 后剩余: {}, 丢弃: {}",
                    before_count, TVL_THRESHOLD, after_count, before_count - after_count);
            }
            
            if !batch_pools.is_empty() {
                if let Err(e) = db.insert_batch(&batch_pools) {
                    error!("❌ 数据库写入失败: {:?}", e);
                    return Err(e.into());
                }
                total_saved += batch_pools.len();
                pb.inc(batch_pools.len() as u64);
            }
            pb.set_message(format!("{}", skip));
            
            if page_count % 5 == 0 {
                info!("第 {} 页完成 | Skip: {} | 已存总数: {}", page_count, skip, total_saved);
            }
        }

        skip += BATCH_SIZE;

        if fetched_count < BATCH_SIZE {
            info!("✅ 已获取该 DEX 全部数据 (最后一批不足 {} 条)", BATCH_SIZE);
            break;
        }

        sleep(Duration::from_millis(200)).await;
    }

    pb.finish_with_message(format!("任务完成，共存入 {} 条数据", total_saved));
    info!("✅ [{:?}] 拉取结束，有效数据: {}", protocol, total_saved);
    Ok(())
}

/// 将 Subgraph 返回的 decimals 字符串（BigInt）解析为 u8，缺失时返回 None
fn parse_decimals(raw: &Option<String>) -> Option<u8> {
    raw.as_ref()?.parse::<u8>().ok()
}

fn parse_response(protocol: &Protocol, body: Value) -> Result<(Vec<UnifiedPool>, usize)> {
    let mut pools = Vec::new();
    let count;

    match protocol {
        // UniV3: tickSpacing 字段已移除（部分 endpoint 不支持），UniV3 全部为 vAMM
        Protocol::UniV3 => {
            let data: GraphResponse<V3Data> = serde_json::from_value(body).context("解析 UniV3 数据结构失败")?;
            count = data.data.pools.len();
            for p in data.data.pools {
                let tvl = p.totalValueLockedUSD.parse::<f64>().unwrap_or(0.0);
                let fee = p.feeTier.parse::<u32>().unwrap_or(0);
                let raw = serde_json::to_string(&p).unwrap();

                let extra = serde_json::json!({
                    "liquidity": p.liquidity,
                    "tvl_usd": p.totalValueLockedUSD,
                    "amm_type": "vAMM"
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol,
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol,
                    fee,
                    decimals0: parse_decimals(&p.token0.decimals),
                    decimals1: parse_decimals(&p.token1.decimals),
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        },
        // AerodromeV3: 没有 tickSpacing 字段，使用 feeTier 区分 sAMM/vAMM
        Protocol::AerodromeV3 => {
            let pools_arr = body["data"]["pools"]
                .as_array()
                .context("AerodromeV3: 缺少 data.pools 数组")?;
            count = pools_arr.len();
            for p in pools_arr {
                let id = p["id"].as_str().unwrap_or("").to_string();
                let fee_str = p["feeTier"].as_str().unwrap_or("0");
                let fee = fee_str.parse::<u32>().unwrap_or(0);
                let liq = p["liquidity"].as_str().unwrap_or("0").to_string();
                let tvl_str = p["totalValueLockedUSD"].as_str().unwrap_or("0");
                let tvl = tvl_str.parse::<f64>().unwrap_or(0.0);
                let raw = p.to_string();

                let t0_id = p["token0"]["id"].as_str().unwrap_or("").to_string();
                let t0_sym = p["token0"]["symbol"].as_str().unwrap_or("").to_string();
                let t0_dec = p["token0"]["decimals"].as_str().map(|s| s.parse::<u8>().ok()).flatten();
                let t1_id = p["token1"]["id"].as_str().unwrap_or("").to_string();
                let t1_sym = p["token1"]["symbol"].as_str().unwrap_or("").to_string();
                let t1_dec = p["token1"]["decimals"].as_str().map(|s| s.parse::<u8>().ok()).flatten();

                // Aerodrome sAMM/vAMM 区分：sAMM 稳定池 fee 极低 (1~5 bps)，vAMM 波动池 fee 较高
                let amm_type = if fee <= 5 { "sAMM" } else { "vAMM" };

                let extra = serde_json::json!({
                    "liquidity": liq,
                    "tvl_usd": tvl_str,
                    "amm_type": amm_type
                }).to_string();

                pools.push(UnifiedPool {
                    id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: t0_id,
                    token0_symbol: t0_sym,
                    token1_id: t1_id,
                    token1_symbol: t1_sym,
                    fee,
                    decimals0: t0_dec,
                    decimals1: t1_dec,
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        },
        Protocol::UniV2 => {
            let data: GraphResponse<V2Data> = serde_json::from_value(body).context("解析 V2 数据结构失败")?;
            count = data.data.pairs.len();
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
                    decimals0: parse_decimals(&p.token0.decimals),
                    decimals1: parse_decimals(&p.token1.decimals),
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        },
        Protocol::UniV4 | Protocol::PancakeV4 => {
            let data: GraphResponse<V4Data> = serde_json::from_value(body).context("解析 V4 数据结构失败")?;
            count = data.data.pools.len();
            for p in data.data.pools {
                let tvl = p.totalValueLockedUSD.parse::<f64>().unwrap_or(0.0);
                let fee = p.feeTier.parse::<u32>().unwrap_or(0);
                let raw = serde_json::to_string(&p).unwrap();
                // V4 特有：将 hooks 放入 extra_data，并标注版本
                let extra = serde_json::json!({
                    "liquidity": p.liquidity,
                    "tvl_usd": p.totalValueLockedUSD,
                    "hooks": p.hooks,
                    "version": "v4"
                }).to_string();

                pools.push(UnifiedPool {
                    id: p.id,
                    protocol: protocol.as_str().to_string(),
                    token0_id: p.token0.id,
                    token0_symbol: p.token0.symbol,
                    token1_id: p.token1.id,
                    token1_symbol: p.token1.symbol,
                    fee,
                    decimals0: parse_decimals(&p.token0.decimals),
                    decimals1: parse_decimals(&p.token1.decimals),
                    raw_json: raw,
                    extra_data: extra,
                    tvl_usd: tvl,
                });
            }
        }
    }
    Ok((pools, count))
}

fn build_query(protocol: &Protocol, skip: usize, first: usize) -> String {
    match protocol {
        // UniV3: tickSpacing 字段已移除（部分 endpoint 不支持该字段，且 UniV3 不需要区分 sAMM/vAMM）
        Protocol::UniV3 => format!(
            r#"{{
                pools(first: {}, skip: {}, orderBy: totalValueLockedUSD, orderDirection: desc) {{
                    id
                    feeTier
                    liquidity
                    totalValueLockedUSD
                    token0 {{ id symbol decimals }}
                    token1 {{ id symbol decimals }}
                }}
            }}"#,
            first, skip
        ),
        // Aerodrome subgraph 的 Pool 实体没有 tickSpacing 字段，不能查询！
        Protocol::AerodromeV3 => format!(
            r#"{{
                pools(first: {}, skip: {}, orderBy: totalValueLockedUSD, orderDirection: desc) {{
                    id
                    feeTier
                    liquidity
                    totalValueLockedUSD
                    token0 {{ id symbol decimals }}
                    token1 {{ id symbol decimals }}
                }}
            }}"#,
            first, skip
        ),
        Protocol::UniV2 => format!(
            r#"{{
                pairs(first: {}, skip: {}, orderBy: reserveUSD, orderDirection: desc) {{
                    id
                    reserveUSD
                    token0 {{ id symbol decimals }}
                    token1 {{ id symbol decimals }}
                }}
            }}"#,
            first, skip
        ),
        Protocol::UniV4 | Protocol::PancakeV4 => format!(
            r#"{{
                pools(first: {}, skip: {}, orderBy: totalValueLockedUSD, orderDirection: desc) {{
                    id
                    feeTier
                    hooks
                    liquidity
                    totalValueLockedUSD
                    token0 {{ id symbol decimals }}
                    token1 {{ id symbol decimals }}
                }}
            }}"#,
            first, skip
        ),
    }
}