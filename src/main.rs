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
use std::io::Write;
use log::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 初始化日志系统 (带时间戳)
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .init();
    
    info!("=== 程序启动 ===");

    // 2. 加载配置
    let config = Config::load()?;
    info!("配置加载成功，数据库路径: {}", config.db_path);

    // 3. 初始化数据库 (Db::new 内部已开启 WAL 模式)
    let mut database = Db::new(&config.db_path)?;
    database.init()?;

    // 4. 初始化 HTTP 客户端
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // 5. 定义任务列表
    let tasks = vec![
        (config.url_uni_v3, Protocol::UniV3),
        (config.url_aerodrome, Protocol::AerodromeV3),
        (config.url_uni_v2, Protocol::UniV2),
    ];

    // 6. 第一步：流式拉取 + 粗筛选 + 批量入库
    // 这一步会直接将过滤后的数据写入 raw_pools 表，避免内存溢出
    info!("--- 第一步：流式拉取与粗筛选 (TVL > $5000) ---");
    for (url, protocol) in tasks {
        match fetcher::fetch_and_save(&client, &mut database, &url, protocol.clone()).await {
            Ok(_) => info!("{:?} 任务阶段完成", protocol),
            Err(e) => error!("{:?} 任务失败: {:?}", protocol, e),
        }
    }

    // 7. 第二步：读取所有粗数据
    // 此时读取的数据量已经远小于原始全量数据
    info!("--- 第二步：读取已入库的粗数据 ---");
    let raw_pools = database.get_all_raw()?;
    info!("数据库中粗数据总条数: {}", raw_pools.len());

    // 8. 第三步：精细清洗 (剔除单交易对代币)
    info!("--- 第三步：精处理 (剔除单交易对地址) ---");
    let clean_pools = cleaner::clean_pools(raw_pools)?;
    info!("清洗后剩余目标数据: {} 条", clean_pools.len());

    // 9. 第四步：写入目标数据库 (Target DB)
    info!("--- 第四步：生成目标地址库 ---");
    database.clear_target()?;
    
    // 将清洗后的最终数据写入 target_pools 表
    // 由于经过了 TVL 过滤和清洗，数据量通常在几千到几万条，循环插入速度可以接受
    let mut count = 0;
    for pool in &clean_pools {
        database.insert_target(pool)?;
        count += 1;
    }
    info!("已成功写入 {} 条目标数据", count);

    info!("✅ 所有任务完成！请检查 {}", config.db_path);
    Ok(())
}