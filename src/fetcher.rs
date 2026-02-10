use reqwest::Client;
use serde_json::Value;
use anyhow::{Result, Context};
use crate::models::{Protocol, UnifiedPool, GraphResponse, V3Data, V2Data};
use indicatif::{ProgressBar, ProgressStyle};

const BATCH_SIZE: usize = 1000;

pub async fn fetch_all(
    client: &Client, 
    url: &str, 
    protocol: Protocol
) -> Result<Vec<UnifiedPool>> {
    let mut all_pools = Vec::new();
    let mut last_id = "".to_string();
    let mut has_more = true;

    println!("开始拉取 [{:?}] 数据...", protocol);
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} 已拉取: {pos} 条 | 上次ID: {msg}").unwrap());

    while has_more {
        let query = build_query(&protocol, &last_id);
        let resp_body: Value = client.post(url)
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await?
            .json()
            .await?;
        
        // 检查是否有 errors
        if let Some(errs) = resp_body.get("errors") {
            pb.finish_with_message("Query Error");
            return Err(anyhow::anyhow!("GraphQL Error: {:?}", errs));
        }

        let fetched_count = match protocol {
            Protocol::UniV3 | Protocol::AerodromeV3 => {
                let data: GraphResponse<V3Data> = serde_json::from_value(resp_body).context("解析 V3 数据失败")?;
                let batch_len = data.data.pools.len();
                if batch_len > 0 {
                    last_id = data.data.pools.last().unwrap().id.clone();
                    for p in data.data.pools {
                        // 尝试解析 fee, 如果是 Aerodrome 可能是 tickSpacing 换算，这里简单处理
                        let fee = p.feeTier.parse::<u32>().unwrap_or(0);
                        
                        let extra = serde_json::json!({
                            "liquidity": p.liquidity,
                            "symbol0": p.token0.symbol,
                            "symbol1": p.token1.symbol
                        }).to_string();

                        // 备份原始 json
                        let raw = serde_json::to_string(&p).unwrap();

                        all_pools.push(UnifiedPool {
                            id: p.id,
                            protocol: protocol.as_str().to_string(),
                            token0_id: p.token0.id,
                            token0_symbol: p.token0.symbol,
                            token1_id: p.token1.id,
                            token1_symbol: p.token1.symbol,
                            fee,
                            raw_json: raw,
                            extra_data: extra,
                        });
                    }
                }
                batch_len
            },
            Protocol::UniV2 => {
                let data: GraphResponse<V2Data> = serde_json::from_value(resp_body).context("解析 V2 数据失败")?;
                let batch_len = data.data.pairs.len();
                if batch_len > 0 {
                    last_id = data.data.pairs.last().unwrap().id.clone();
                    for p in data.data.pairs {
                        let extra = serde_json::json!({
                            "reserveUSD": p.reserveUSD,
                            "symbol0": p.token0.symbol,
                            "symbol1": p.token1.symbol
                        }).to_string();
                        let raw = serde_json::to_string(&p).unwrap();

                        all_pools.push(UnifiedPool {
                            id: p.id,
                            protocol: protocol.as_str().to_string(),
                            token0_id: p.token0.id,
                            token0_symbol: p.token0.symbol,
                            token1_id: p.token1.id,
                            token1_symbol: p.token1.symbol,
                            fee: 3000, // V2 默认 0.3%
                            raw_json: raw,
                            extra_data: extra,
                        });
                    }
                }
                batch_len
            }
        };

        pb.set_message(last_id.clone());
        pb.inc(fetched_count as u64);

        if fetched_count < BATCH_SIZE {
            has_more = false;
        }
    }

    pb.finish_with_message("拉取完成");
    Ok(all_pools)
}

fn build_query(protocol: &Protocol, last_id: &str) -> String {
    match protocol {
        Protocol::UniV3 | Protocol::AerodromeV3 => format!(
            r#"{{
                pools(first: {}, where: {{ id_gt: "{}" }}, orderBy: id, orderDirection: asc) {{
                    id
                    feeTier
                    liquidity
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