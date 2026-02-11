use std::collections::HashMap;
use crate::models::UnifiedPool;
use anyhow::Result;
use indicatif::ProgressBar;

pub fn clean_pools(pools: Vec<UnifiedPool>) -> Result<Vec<UnifiedPool>> {
    println!("🧹 开始执行深度清洗...");
    println!("📊 原始数据条数: {}", pools.len());

    let mut token_counts: HashMap<String, usize> = HashMap::new();

    for pool in &pools {
        *token_counts.entry(pool.token0_id.clone()).or_insert(0) += 1;
        *token_counts.entry(pool.token1_id.clone()).or_insert(0) += 1;
    }

    let pb = ProgressBar::new(pools.len() as u64);
    let mut valid_pools = Vec::new();

    for pool in pools {
        let count0 = token_counts.get(&pool.token0_id).unwrap_or(&0);
        let count1 = token_counts.get(&pool.token1_id).unwrap_or(&0);

        if *count0 > 1 && *count1 > 1 {
            valid_pools.push(pool);
        }
        pb.inc(1);
    }

    pb.finish_and_clear();
    println!("✨ 清洗完成");
    println!("✅ 剩余有效池子: {}", valid_pools.len());
    
    Ok(valid_pools)
}