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
                raw_json TEXT
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS target_pools (
                address TEXT PRIMARY KEY,
                protocol TEXT NOT NULL,
                token0 TEXT NOT NULL,
                token0_symbol TEXT DEFAULT '',
                token1 TEXT NOT NULL,
                token1_symbol TEXT DEFAULT '',
                fee INTEGER DEFAULT 0,
                extra_data TEXT
            )",
            [],
        )?;

        Ok(())
    }

    pub fn insert_batch(&mut self, pools: &[UnifiedPool]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO raw_pools 
                (id, protocol, token0, token0_symbol, token1, token1_symbol, tvl_usd, raw_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
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
                    pool.raw_json
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_all_raw(&self) -> Result<Vec<UnifiedPool>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, protocol, token0, token0_symbol, token1, token1_symbol, tvl_usd, raw_json FROM raw_pools"
        )?;
        
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let protocol: String = row.get(1)?;
            let raw_json: String = row.get(7)?;
            
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

            // 2. 重建 extra_data (包含 hooks)
            let extra_data = if protocol == "V4" || protocol == "Pancake-V4" {
                serde_json::json!({
                    "hooks": val["hooks"], 
                    "liquidity": val["liquidity"],
                    "tvl_usd": val["totalValueLockedUSD"]
                }).to_string()
            } else if protocol == "V2" {
                serde_json::json!({
                    "reserveUSD": val["reserveUSD"]
                }).to_string()
            } else {
                // V3 & Aerodrome
                serde_json::json!({
                    "liquidity": val["liquidity"],
                    "tvl_usd": val["totalValueLockedUSD"]
                }).to_string()
            };

            Ok(UnifiedPool {
                id,
                protocol,
                token0_id: row.get(2)?,
                token0_symbol: row.get(3)?,
                token1_id: row.get(4)?,
                token1_symbol: row.get(5)?,
                fee,
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