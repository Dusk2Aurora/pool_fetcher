use rusqlite::{params, Connection, Result};
use crate::models::{UnifiedPool, Tick};
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

            // 2. V4 地址截断与额外数据处理
            let mut final_address = id.clone();
            
            let extra_data = if protocol == "V4" || protocol == "Pancake-V4" {
                // 截断 id 为 20 bytes (40 chars)
                let hex_id = id.trim_start_matches("0x");
                if hex_id.len() >= 40 {
                    final_address = format!("0x{}", &hex_id[hex_id.len() - 40..]).to_lowercase();
                } else {
                    final_address = format!("0x{:0>40}", hex_id).to_lowercase();
                }

                serde_json::json!({
                    "hooks": val["hooks"], 
                    "liquidity": val["liquidity"],
                    "tvl_usd": val["totalValueLockedUSD"],
                    "pool_id": id // 保存原始 32 bytes ID
                }).to_string()
            } else if protocol == "V2" {
                final_address = final_address.to_lowercase();
                serde_json::json!({
                    "reserveUSD": val["reserveUSD"]
                }).to_string()
            } else {
                // V3 & Aerodrome-V3
                final_address = final_address.to_lowercase();
                serde_json::json!({
                    "liquidity": val["liquidity"],
                    "tvl_usd": val["totalValueLockedUSD"],
                    "tick_spacing": val["tickSpacing"]
                }).to_string()
            };

            Ok(UnifiedPool {
                id: final_address,
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

    pub fn insert_ticks_batch(&mut self, pool_address: &str, block_height: i64, ticks: &[Tick]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO pool_ticks 
                (pool_address, tick_idx, liquidity_gross, liquidity_net, price0, price1, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            
            for tick in ticks {
                let tick_idx = tick.tickIdx.parse::<i64>().unwrap_or(0);
                stmt.execute(params![
                    pool_address,
                    tick_idx,
                    tick.liquidityGross,
                    tick.liquidityNet,
                    tick.price0,
                    tick.price1,
                    block_height
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Get pools with pagination for tick fetching
    /// Returns pools starting from start_address (exclusive), ordered by address
    pub fn get_pools_for_tick_fetch(&self, start_address: Option<&str>, limit: usize) -> Result<Vec<UnifiedPool>> {
        let sql = match start_address {
            Some(_) => "SELECT address, protocol, token0, token0_symbol, token1, token1_symbol, fee, extra_data FROM target_pools WHERE address > ?1 ORDER BY address ASC LIMIT ?2",
            None => "SELECT address, protocol, token0, token0_symbol, token1, token1_symbol, fee, extra_data FROM target_pools ORDER BY address ASC LIMIT ?1",
        };

        let mut stmt = self.conn.prepare(sql)?;
        
        let pools: Vec<UnifiedPool> = match start_address {
            Some(addr) => {
                stmt.query_map(params![addr, limit], |row| {
                    Ok(UnifiedPool {
                        id: row.get(0)?,
                        protocol: row.get(1)?,
                        token0_id: row.get(2)?,
                        token0_symbol: row.get(3)?,
                        token1_id: row.get(4)?,
                        token1_symbol: row.get(5)?,
                        fee: row.get(6)?,
                        tvl_usd: 0.0,
                        raw_json: String::new(),
                        extra_data: row.get(7)?,
                    })
                })?.filter_map(|r| r.ok()).collect()
            },
            None => {
                stmt.query_map(params![limit as i64], |row| {
                    Ok(UnifiedPool {
                        id: row.get(0)?,
                        protocol: row.get(1)?,
                        token0_id: row.get(2)?,
                        token0_symbol: row.get(3)?,
                        token1_id: row.get(4)?,
                        token1_symbol: row.get(5)?,
                        fee: row.get(6)?,
                        tvl_usd: 0.0,
                        raw_json: String::new(),
                        extra_data: row.get(7)?,
                    })
                })?.filter_map(|r| r.ok()).collect()
            }
        };
        
        Ok(pools)
    }

    /// Count total pools for progress bar
    pub fn count_pools(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM target_pools",
            [],
            |row| row.get(0)
        )?;
        Ok(count as usize)
    }
}

/// Separate database for storing tick data
pub struct TicksDb {
    pub conn: Connection,
}

impl TicksDb {
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
            "CREATE TABLE IF NOT EXISTS pool_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_address TEXT NOT NULL,
                tick_idx INTEGER NOT NULL,
                liquidity_gross TEXT NOT NULL,
                liquidity_net TEXT NOT NULL,
                price0 TEXT NOT NULL,
                price1 TEXT NOT NULL,
                block_height INTEGER NOT NULL,
                UNIQUE(pool_address, tick_idx, block_height)
            )",
            [],
        )?;

        Ok(())
    }

    pub fn insert_ticks_batch(&mut self, pool_address: &str, block_height: i64, ticks: &[Tick]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO pool_ticks 
                (pool_address, tick_idx, liquidity_gross, liquidity_net, price0, price1, block_height)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;
            
            for tick in ticks {
                let tick_idx = tick.tickIdx.parse::<i64>().unwrap_or(0);
                stmt.execute(params![
                    pool_address,
                    tick_idx,
                    tick.liquidityGross,
                    tick.liquidityNet,
                    tick.price0,
                    tick.price1,
                    block_height
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}