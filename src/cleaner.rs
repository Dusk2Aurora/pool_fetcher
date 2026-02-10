use std::collections::HashMap;
use crate::models::UnifiedPool;
use anyhow::Result;
use indicatif::ProgressBar;
use serde_json::Value;

pub fn clean_pools(pools: Vec<UnifiedPool>) -> Result<Vec<UnifiedPool>> {
    println!("开始执行数据清洗...");
    println!("原始数据条数: {}", pools.len());

    // 1. 统计每个 Token 出现的频率 (Degree)
    let mut token_counts: HashMap<String, usize> = HashMap::new();

    for pool in &pools {
        *token_counts.entry(pool.token0_id.clone()).or_insert(0) += 1;
        *token_counts.entry(pool.token1_id.clone()).or_insert(0) += 1;
    }

    let pb = ProgressBar::new(pools.len() as u64);
    let mut valid_pools = Vec::new();

    // 2. 剔除逻辑
    // 如果一个池子包含的任意一个 Token，在全网所有池子中只出现过这一次 (count == 1)
    // 则说明该 Token 只有这一个交易对，无法形成闭环套利，剔除该池子。
    for pool in pools {
        let count0 = token_counts.get(&pool.token0_id).unwrap_or(&0);
        let count1 = token_counts.get(&pool.token1_id).unwrap_or(&0);

        if *count0 > 1 && *count1 > 1 {
            // 需要重新解析 raw_json 以补全信息写入 target 库吗？
            // 在 fetcher 阶段我们已经填入了大部分信息，这里只需透传
            // 但如果 raw_json 里有更多 fee/extra 信息，可以在这里解析并更新 struct
            
            // 简单处理：如果字段缺失去 raw_json 找 (针对从 DB 读出来的情况)
            let mut final_pool = pool.clone();
            
            if final_pool.extra_data.is_empty() {
                // 尝试从 raw_json 恢复 extra_data (示例逻辑)
                if let Ok(v) = serde_json::from_str::<Value>(&final_pool.raw_json) {
                     // 这里可以根据 protocol 重新提取数据，演示略
                }
            }
            
            valid_pools.push(final_pool);
        }
        pb.inc(1);
    }

    pb.finish_and_clear();
    println!("清洗后剩余条数: {}", valid_pools.len());
    println!("剔除条数: {}", pb.length().unwrap() - valid_pools.len() as u64);

    Ok(valid_pools)
}