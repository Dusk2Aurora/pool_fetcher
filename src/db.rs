use rusqlite::{params, Connection, Result};
use crate::models::UnifiedPool;

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
        // 1. 粗数据表
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

        // 2. 目标表
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
            Ok(UnifiedPool {
                id: row.get(0)?,
                protocol: row.get(1)?,
                token0_id: row.get(2)?,
                token0_symbol: row.get(3)?,
                token1_id: row.get(4)?,
                token1_symbol: row.get(5)?,
                fee: 0, 
                tvl_usd: row.get(6)?,
                raw_json: row.get(7)?,
                extra_data: "".to_string(),
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