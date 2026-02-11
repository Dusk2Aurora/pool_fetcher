use reqwest::Client;
use serde_json::Value;
use anyhow::{Result, Context};
use crate::models::{Protocol, UnifiedPool, GraphResponse, V3Data, V2Data};
use crate::db::Db; // 引入 DB
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn};

const BATCH_SIZE: usize = 1000;
const TVL_THRESHOLD: f64 = 5000.0; // 5000 USD 门槛

// 注意：这里函数签名变了，接收 &mut Db
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
        
        // 发送请求
        let resp = client.post(url)
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await?;
            
        let resp_body: Value = resp.json().await?;

        if let Some(errs) = resp_body.get("errors") {
            warn!("GraphQL Error: {:?}", errs);
            return Err(anyhow::anyhow!("GraphQL Error"));
        }

        // 解析并处理数据
        let (mut batch_pools, fetched_count, new_last_id) = parse_response(&protocol, resp_body)?;

        if fetched_count > 0 {
            last_id = new_last_id;
            
            // --- 核心改动：粗筛选 ---
            batch_pools.retain(|p| p.tvl_usd >= TVL_THRESHOLD);
            
            // --- 核心改动：立即写入 ---
            if !batch_pools.is_empty() {
                db.insert_batch(&batch_pools)?;
                total_saved += batch_pools.len();
                pb.inc(batch_pools.len() as u64);
            }
            pb.set_message(last_id.clone());
        }

        if fetched_count < BATCH_SIZE {
            has_more = false;
        }
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
            let data: GraphResponse<V3Data> = serde_json::from_value(body).context("解析 V3 失败")?;
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
            let data: GraphResponse<V2Data> = serde_json::from_value(body).context("解析 V2 失败")?;
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