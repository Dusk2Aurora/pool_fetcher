use reqwest::Client;
use serde_json::Value;
use anyhow::{Result, Context};
use crate::models::{Protocol, UnifiedPool, GraphResponse, V3Data, V2Data, V4Data, TicksData};
use crate::db::{Db, TicksDb};
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use log::{info, warn, error};
use tokio::time::{sleep, Duration};
use std::time::Instant;

const BATCH_SIZE: usize = 1000;
const TVL_THRESHOLD: f64 = 1.0;
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
        Protocol::UniV4 | Protocol::PancakeV4 => {
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
        Protocol::UniV4 | Protocol::PancakeV4 => format!(
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

// ==================== Tick Fetcher ====================

const TICK_BATCH_SIZE: usize = 1000;
const TICK_MAX_RETRIES: u32 = 5;

pub async fn fetch_ticks_and_save(
    client: &Client,
    ticks_db: &mut TicksDb,
    url: &str,
    pool_address: &str,
    block_height: u64,
) -> Result<()> {
    let start_time = Instant::now();
    let mut last_tick_idx = String::new();
    let mut has_more = true;
    let mut total_ticks = 0;
    let mut page_count = 0;

    info!("🚀 开始拉取 Pool Ticks");
    info!("🔗 池子地址: {}", pool_address);
    info!("📦 目标区块高度: {}", block_height);

    // 创建多进度条
    let multi_pb = MultiProgress::new();
    
    // 主进度条 - 显示总体进度
    let main_pb = multi_pb.add(ProgressBar::new(0));
    main_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ticks | 当前TickIdx: {msg} | 耗时: {duration}")
            .unwrap()
            .progress_chars("━━╸"),
    );
    main_pb.set_message(String::from("-"));

    while has_more {
        page_count += 1;
        let query = build_tick_query(pool_address, &last_tick_idx, block_height);

        let resp_body: Value = fetch_with_retry(client, url, &query, "Ticks").await?;

        // 检查 errors
        if let Some(errs) = resp_body.get("errors") {
            warn!("⚠️ GraphQL 返回错误信息: {:?}", errs);
        }

        // 检查 data
        if resp_body.get("data").is_none() || resp_body["data"].is_null() {
            error!("❌ 致命错误：服务器响应缺少 'data' 字段！");
            return Err(anyhow::anyhow!("GraphQL Error: Missing data field"));
        }

        let ticks_data: TicksData = serde_json::from_value(resp_body["data"].clone())
            .context("解析 Tick 数据结构失败")?;

        let fetched_count = ticks_data.ticks.len();

        if fetched_count > 0 {
            // 获取最后一个 tick 的索引用于分页
            if let Some(last_tick) = ticks_data.ticks.last() {
                last_tick_idx = last_tick.tickIdx.clone();
            }

            // 写入数据库
            if let Err(e) = ticks_db.insert_ticks_batch(pool_address, block_height as i64, &ticks_data.ticks) {
                error!("❌ 数据库写入失败: {:?}", e);
                return Err(e.into());
            }

            total_ticks += fetched_count;
            
            // 更新主进度条
            main_pb.set_message(last_tick_idx.clone());
            main_pb.set_length(total_ticks as u64);
            main_pb.set_position(total_ticks as u64);

            if page_count % 5 == 0 {
                let elapsed = start_time.elapsed();
                info!("第 {} 页完成 | 当前 TickIdx: {} | 已存总数: {} | 耗时: {:.2}s", 
                    page_count, last_tick_idx, total_ticks, elapsed.as_secs_f64());
            }
        } else {
            has_more = false;
        }

        if fetched_count < TICK_BATCH_SIZE {
            has_more = false;
        }

        // 避免请求过快
        sleep(Duration::from_millis(200)).await;
    }

    main_pb.finish_with_message(format!("完成 (TickIdx: {})", last_tick_idx));
    
    let elapsed = start_time.elapsed();
    info!("✅ 拉取结束，共存入 {} 条 Ticks 数据 | 总耗时: {:.2}s", total_ticks, elapsed.as_secs_f64());
    
    Ok(())
}

fn build_tick_query(pool_address: &str, last_tick_idx: &str, block_height: u64) -> String {
    let tick_idx_filter = if last_tick_idx.is_empty() {
        String::new()
    } else {
        format!(", tickIdx_gt: \"{}\"", last_tick_idx)
    };

    format!(
        r#"{{
            ticks(
                first: {},
                where: {{ pool: "{}"{} }},
                orderBy: tickIdx,
                orderDirection: asc,
                block: {{ number: {} }}
            ) {{
                tickIdx
                liquidityGross
                liquidityNet
                price0
                price1
            }}
        }}"#,
        TICK_BATCH_SIZE, pool_address, tick_idx_filter, block_height
    )
}

async fn fetch_with_retry(client: &Client, url: &str, query: &str, data_type: &str) -> Result<Value> {
    let mut attempts = 0;
    
    loop {
        attempts += 1;
        let result = client.post(url)
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await;

        match result {
            Ok(resp) => {
                match resp.json::<Value>().await {
                    Ok(body) => return Ok(body),
                    Err(e) => {
                        if attempts >= TICK_MAX_RETRIES {
                            error!("❌ 解析 JSON 失败 ({} - 重试耗尽): {:?}", data_type, e);
                            return Err(e.into());
                        }
                        warn!("⚠️ 解析 JSON 失败 ({} - 第 {} 次重试): {:?}", data_type, attempts, e);
                        sleep(Duration::from_secs(2u64.pow(attempts.min(5)))).await;
                    }
                }
            },
            Err(e) => {
                if attempts >= TICK_MAX_RETRIES {
                    error!("❌ 网络请求失败 ({} - 重试耗尽): {:?}", data_type, e);
                    return Err(e.into());
                }
                warn!("⚠️ 网络请求失败 ({} - 第 {} 次重试): {:?}。等待重试...", data_type, attempts, e);
                sleep(Duration::from_secs(2u64.pow(attempts.min(5)))).await;
            }
        }
    }
}

/// Query _meta to get current block height
pub async fn fetch_current_block_height(client: &Client, url: &str) -> Result<u64> {
    let query = r#"{
        _meta {
            block {
                number
            }
        }
    }"#;

    let resp_body = fetch_with_retry(client, url, query, "_meta").await?;
    
    if let Some(errs) = resp_body.get("errors") {
        warn!("⚠️ _meta 查询返回错误: {:?}", errs);
    }
    
    if resp_body.get("data").is_none() || resp_body["data"].is_null() {
        return Err(anyhow::anyhow!("_meta query failed: missing data field"));
    }
    
    let block_number = resp_body["data"]["_meta"]["block"]["number"]
        .as_u64()
        .context("Failed to parse block number from _meta response")?;
    
    info!("📦 当前区块高度: {}", block_number);
    Ok(block_number)
}

// ==================== Multi-Pool Tick Fetcher ====================

const POOL_BATCH_SIZE: usize = 100;

pub async fn fetch_ticks_for_all_pools(
    client: &Client,
    db: &Db,
    ticks_db: &mut TicksDb,
    url: &str,
    start_pool_address: Option<String>,
    block_height: Option<u64>,
) -> Result<()> {
    // Setup progress bar FIRST so we can use it throughout
    let multi_pb = MultiProgress::new();
    
    // Count total pools for progress
    let total_pools = db.count_pools()?;
    multi_pb.println(format!("📊 总共需要处理 {} 个池子", total_pools))?;
    
    // Pool progress bar
    let pool_pb = multi_pb.add(ProgressBar::new(total_pools as u64));
    pool_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] 池子进度: [{bar:40.cyan/blue}] {pos}/{len} | 当前: {msg}")
            .unwrap()
            .progress_chars("━━╸"),
    );
    pool_pb.set_message(String::from("-"));
    
    // Tick progress bar (shows current pool's tick progress)
    let tick_pb = multi_pb.add(ProgressBar::new(0));
    tick_pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.yellow} [{elapsed_precise}] Ticks: {msg}")
            .unwrap(),
    );
    tick_pb.set_message(String::from("-"));
    
    // If block_height not provided, fetch from _meta
    let actual_block_height = match block_height {
        Some(h) => {
            multi_pb.println(format!("📦 使用指定的区块高度: {}", h))?;
            h
        },
        None => {
            multi_pb.println("📦 未指定区块高度，正在从 _meta 查询当前区块高度...")?;
            fetch_current_block_height(client, url).await?
        }
    };
    
    let start_time = Instant::now();
    let mut last_pool_address = start_pool_address;
    let mut total_ticks_fetched = 0;
    let mut pools_processed = 0;
    
    loop {
        // Fetch batch of pools
        let pools = db.get_pools_for_tick_fetch(last_pool_address.as_deref(), POOL_BATCH_SIZE)?;
        
        if pools.is_empty() {
            multi_pb.println("✅ 所有池子处理完成")?;
            break;
        }
        
        for pool in &pools {
            pools_processed += 1;
            pool_pb.set_message(pool.id.clone());
            pool_pb.inc(1);
            
            // Check if we should skip V2 pools (no tick data)
            if pool.protocol == "V2" {
                tick_pb.println(format!("⏭️ 跳过 V2 池子 (无 tick 数据): {}", pool.id));
                last_pool_address = Some(pool.id.clone());
                continue;
            }
            
            // Fetch ticks for this pool
            tick_pb.set_message(format!("正在拉取 {}...", pool.id));
            
            match fetch_ticks_single_pool(client, ticks_db, url, &pool.id, actual_block_height).await {
                Ok(tick_count) => {
                    total_ticks_fetched += tick_count;
                    tick_pb.set_message(format!("✓ {}: {} ticks", pool.id, tick_count));
                },
                Err(e) => {
                    error!("❌ 拉取池子 {} 的 tick 失败: {:?}", pool.id, e);
                    tick_pb.set_message(format!("✗ {} 失败", pool.id));
                }
            }
            
            last_pool_address = Some(pool.id.clone());
            
            // Small delay to avoid overwhelming the endpoint
            sleep(Duration::from_millis(100)).await;
        }
        
        // If we got less than batch size, we're done
        if pools.len() < POOL_BATCH_SIZE {
            break;
        }
    }
    
    pool_pb.finish_with_message(format!("完成 ({} 个池子)", pools_processed));
    tick_pb.finish_with_message(String::from("完成"));
    
    let elapsed = start_time.elapsed();
    multi_pb.println("✅ 多池 Tick 拉取完成！")?;
    multi_pb.println(format!("📊 总共处理: {} 个池子, {} 条 ticks | 总耗时: {:.2}s", 
        pools_processed, total_ticks_fetched, elapsed.as_secs_f64()))?;
    
    Ok(())
}

/// Fetch ticks for a single pool with internal progress tracking
async fn fetch_ticks_single_pool(
    client: &Client,
    ticks_db: &mut TicksDb,
    url: &str,
    pool_address: &str,
    block_height: u64,
) -> Result<usize> {
    let mut last_tick_idx = String::new();
    let mut has_more = true;
    let mut total_ticks = 0;
    
    while has_more {
        let query = build_tick_query(pool_address, &last_tick_idx, block_height);
        
        let resp_body: Value = fetch_with_retry(client, url, &query, "Ticks").await?;
        
        if let Some(errs) = resp_body.get("errors") {
            warn!("⚠️ GraphQL 返回错误信息: {:?}", errs);
        }
        
        if resp_body.get("data").is_none() || resp_body["data"].is_null() {
            return Err(anyhow::anyhow!("GraphQL Error: Missing data field"));
        }
        
        let ticks_data: TicksData = serde_json::from_value(resp_body["data"].clone())
            .context("解析 Tick 数据结构失败")?;
        
        let fetched_count = ticks_data.ticks.len();
        
        if fetched_count > 0 {
            if let Some(last_tick) = ticks_data.ticks.last() {
                last_tick_idx = last_tick.tickIdx.clone();
            }
            
            ticks_db.insert_ticks_batch(pool_address, block_height as i64, &ticks_data.ticks)?;
            total_ticks += fetched_count;
        } else {
            has_more = false;
        }
        
        if fetched_count < TICK_BATCH_SIZE {
            has_more = false;
        }
        
        sleep(Duration::from_millis(200)).await;
    }
    
    Ok(total_ticks)
}