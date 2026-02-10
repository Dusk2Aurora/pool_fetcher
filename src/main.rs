mod config;
mod db;
mod models;
mod fetcher;
mod cleaner;

use config::Config;
use db::Db;
use models::Protocol;
use reqwest::Client;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    // 1. 加载配置
    let config = Config::load()?;
    println!("配置加载成功，数据库路径: {}", config.db_path);

    // 2. 初始化数据库
    let mut database = Db::new(&config.db_path)?;
    database.init()?;

    // 3. 准备 HTTP 客户端
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // 4. 定义拉取任务
    let tasks = vec![
        (config.url_uni_v3, Protocol::UniV3),
        (config.url_aerodrome, Protocol::AerodromeV3),
        (config.url_uni_v2, Protocol::UniV2),
    ];

    // 5. 第一步：执行拉取并存入 raw_pools
    println!("--- 第一步：拉取数据 ---");
    for (url, protocol) in tasks {
        match fetcher::fetch_all(&client, &url, protocol.clone()).await {
            Ok(pools) => {
                println!("协议 {:?} 拉取到 {} 个池子，正在写入数据库...", protocol, pools.len());
                for pool in pools {
                    database.insert_raw(&pool)?;
                }
            },
            Err(e) => eprintln!("协议 {:?} 拉取失败: {:?}", protocol, e),
        }
    }

    // 6. 第二步 & 第三步：读取原始数据并清洗
    println!("\n--- 第二步：读取原始数据 ---");
    let raw_pools = database.get_all_raw()?;
    
    println!("\n--- 第三步：数据清洗 (剔除单交易对地址) ---");
    let clean_pools = cleaner::clean_pools(raw_pools)?;

    // 7. 第四步：存入目标数据库
    println!("\n--- 第四步：生成目标地址库 ---");
    database.clear_target()?; // 先清空旧数据
    for pool in &clean_pools {
        database.insert_target(pool)?;
    }

    println!("\n✅ 任务完成！目标数据库已生成。");
    Ok(())
}