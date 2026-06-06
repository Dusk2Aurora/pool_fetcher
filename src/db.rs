use rusqlite::{params, Connection, Result};
use crate::models::UnifiedPool;
use serde_json::Value;

pub struct Db {
    pub conn: Connection,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;"
        )?;
        Ok(Self { conn })
    }

    pub fn init(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_pools (
                id TEXT PRIMARY KEY,
                protocol TEXT NOT NULL,
                token0 TEXT NOT NULL,
                token0_symbol TEXT DEFAULT '',
                token1 TEXT NOT NULL,
                token1_symbol TEXT DEFAULT '',
                tvl_usd REAL,
                raw_json TEXT,
                extra_data TEXT,
                decimals0 INTEGER,
                decimals1 INTEGER
            )",
            [],
        )?;

        // 兼容旧表：如果从旧版本升级，尝试添加缺失的 decimals 列
        self.conn.execute(
            "ALTER TABLE raw_pools ADD COLUMN decimals0 INTEGER",
            [],
        ).ok();
        self.conn.execute(
            "ALTER TABLE raw_pools ADD COLUMN decimals1 INTEGER",
            [],
        ).ok();

        // 兼容旧表：添加 extra_data 列
        self.conn.execute(
            "ALTER TABLE raw_pools ADD COLUMN extra_data TEXT",
            [],
        ).ok();

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS target_pools (
                address TEXT PRIMARY KEY,
                protocol TEXT NOT NULL,
                token0 TEXT NOT NULL,
                token0_symbol TEXT DEFAULT '',
                token1 TEXT NOT NULL,
                token1_symbol TEXT DEFAULT '',
                fee INTEGER DEFAULT 0,
                extra_data TEXT,
                decimals0 INTEGER,
                decimals1 INTEGER
            )",
            [],
        )?;

        self.conn.execute(
            "ALTER TABLE target_pools ADD COLUMN decimals0 INTEGER",
            [],
        ).ok();
        self.conn.execute(
            "ALTER TABLE target_pools ADD COLUMN decimals1 INTEGER",
            [],
        ).ok();

        Ok(())
    }

    pub fn insert_batch(&mut self, pools: &[UnifiedPool]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO raw_pools
                (id, protocol, token0, token0_symbol, token1, token1_symbol, tvl_usd, raw_json, extra_data, decimals0, decimals1)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
            )?;
            
            for pool in pools {
                stmt.execute(params![
                    pool.id,
                    pool.protocol,
                    pool.token0_id,
                    pool.token0_symbol,
                    pool.token1_id,
                    pool.token1_symbol,
                    pool.tvl_usd,
                    pool.raw_json,
                    pool.extra_data,
                    pool.decimals0,
                    pool.decimals1,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_all_raw(&self) -> Result<Vec<UnifiedPool>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, protocol, token0, token0_symbol, token1, token1_symbol, tvl_usd, raw_json, extra_data, decimals0, decimals1 FROM raw_pools"
        )?;
        
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let protocol: String = row.get(1)?;
            let raw_json: String = row.get(7)?;
            let stored_extra: Option<String> = row.get(8)?;
            let decimals0: Option<u8> = row.get(9)?;
            let decimals1: Option<u8> = row.get(10)?;
            
            let val: Value = serde_json::from_str(&raw_json).unwrap_or(Value::Null);
            
            // 1. 提取 Fee
            let fee = if protocol == "V2" {
                3000 // V2 固定 0.3%
            } else {
                // V3, Aerodrome, V4, PancakeV4 都有 feeTier
                val["feeTier"]
                    .as_str()
                    .unwrap_or("0")
                    .parse::<u32>()
                    .unwrap_or(0)
            };

            // 2. V4 地址截断
            let mut final_address = id.clone();
            
            if protocol == "V4" || protocol == "Pancake-V4" {
                let hex_id = id.trim_start_matches("0x");
                if hex_id.len() >= 40 {
                    final_address = format!("0x{}", &hex_id[hex_id.len() - 40..]).to_lowercase();
                } else {
                    final_address = format!("0x{:0>40}", hex_id).to_lowercase();
                }
            } else {
                final_address = final_address.to_lowercase();
            }

            // 3. 优先使用存储的 extra_data，回退到从 raw_json 重新生成
            let extra_data = stored_extra.unwrap_or_else(|| {
                if protocol == "V4" || protocol == "Pancake-V4" {
                    serde_json::json!({
                        "hooks": val["hooks"],
                        "liquidity": val["liquidity"],
                        "tvl_usd": val["totalValueLockedUSD"],
                        "pool_id": id
                    }).to_string()
                } else if protocol == "V2" {
                    serde_json::json!({
                        "reserveUSD": val["reserveUSD"]
                    }).to_string()
                } else {
                    serde_json::json!({
                        "liquidity": val["liquidity"],
                        "tvl_usd": val["totalValueLockedUSD"],
                        "tick_spacing": val["tickSpacing"],
                        "amm_type": "vAMM"
                    }).to_string()
                }
            });

            Ok(UnifiedPool {
                id: final_address,
                protocol,
                token0_id: row.get(2)?,
                token0_symbol: row.get(3)?,
                token1_id: row.get(4)?,
                token1_symbol: row.get(5)?,
                fee,
                decimals0,
                decimals1,
                tvl_usd: row.get(6)?,
                raw_json,
                extra_data,
            })
        })?;

        let mut pools = Vec::new();
        for pool in rows {
            pools.push(pool?);
        }
        Ok(pools)
    }

    pub fn clear_target(&self) -> Result<()> {
        self.conn.execute("DELETE FROM target_pools", [])?;
        Ok(())
    }
}