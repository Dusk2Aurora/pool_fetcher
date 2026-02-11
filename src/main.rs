mod config;
mod db;
mod models;
mod fetcher;
mod cleaner;

use clap::{Parser, Subcommand};
use config::Config;
use db::Db;
use models::Protocol;
use reqwest::Client;
use std::time::Duration;
use std::io::Write;
use rusqlite::params;

#[derive(Parser)]
#[command(name = "pool_fetcher")]
#[command(about = "Hermes Subgraph Fetcher Tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 从 Subgraph 拉取数据 (支持断点续传)
    Fetch {
        /// 指定要拉取的协议 (v3, aerodrome, v2)
        #[arg(short, long, value_enum)]
        protocol: Protocol,

        /// 起始 ID (用于断点续传，不填则从头开始)
        #[arg(long)]
        start_id: Option<String>,
    },
    /// 执行深度清洗并生成最终目标表
    Clean,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .init();

    // 加载配置
    let config = Config::load()?;
    let mut database = Db::new(&config.db_path)?;
    database.init()?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch { protocol, start_id } => {
            // 确定目标 URL
            let url = match protocol {
                Protocol::UniV3 => config.url_uni_v3,
                Protocol::AerodromeV3 => config.url_aerodrome,
                Protocol::UniV2 => config.url_uni_v2,
            };

            let client = Client::builder()
                .timeout(Duration::from_secs(60)) // 超时 60秒
                .build()?;

            println!("--- 模式: 单协议拉取 ---");
            if let Some(ref id) = start_id {
                println!("⏩ 启用断点续传，起始 ID: {}", id);
            }

            match fetcher::fetch_and_save(&client, &mut database, &url, protocol.clone(), start_id).await {
                Ok(_) => log::info!("{:?} 任务完成", protocol),
                Err(e) => log::error!("{:?} 任务失败: {:?}", protocol, e),
            }
        }
        Commands::Clean => {
            println!("--- 模式: 深度清洗与目标生成 ---");
            println!("📥 正在读取原始数据 (raw_pools)...");
            let raw_pools = database.get_all_raw()?;
            
            println!("🧼 正在执行清洗算法...");
            let clean_pools = cleaner::clean_pools(raw_pools)?;

            println!("💾 正在生成目标表 (target_pools)...");
            database.clear_target()?;
            
            let tx = database.conn.transaction()?; 
            {
                let mut stmt = tx.prepare(
                    "INSERT OR REPLACE INTO target_pools 
                    (address, protocol, token0, token0_symbol, token1, token1_symbol, fee, extra_data)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
                )?;
                
                for pool in &clean_pools {
                     stmt.execute(params![
                         pool.id, 
                         pool.protocol, 
                         pool.token0_id, 
                         pool.token0_symbol, 
                         pool.token1_id, 
                         pool.token1_symbol, 
                         pool.fee, 
                         pool.extra_data
                     ])?;
                }
            }
            tx.commit()?;
            println!("✅ 清洗完成，目标数据库已更新。");
        }
    }

    Ok(())
}