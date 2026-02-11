use rusqlite::{params, Connection, Result};
use crate::models::UnifiedPool;

pub struct Db {
    conn: Connection,
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
        // 修改 raw_pools 表，增加 tvl_usd 字段方便后续查阅（可选）
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_pools (
                id TEXT PRIMARY KEY,
                protocol TEXT NOT NULL,
                token0 TEXT NOT NULL,
                token1 TEXT NOT NULL,
                tvl_usd REAL,
                raw_json TEXT
            )",
            [],
        )?;

        // target_pools 保持不变
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS target_pools (
                address TEXT PRIMARY KEY,
                protocol TEXT NOT NULL,
                token0 TEXT NOT NULL,
                token1 TEXT NOT NULL,
                fee INTEGER DEFAULT 0,
                extra_data TEXT
            )",
            [],
        )?;
        Ok(())
    }

    // 新增：批量写入方法
    pub fn insert_batch(&mut self, pools: &[UnifiedPool]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO raw_pools (id, protocol, token0, token1, tvl_usd, raw_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;
            
            for pool in pools {
                stmt.execute(params![
                    pool.id, 
                    pool.protocol, 
                    pool.token0_id, 
                    pool.token1_id, 
                    pool.tvl_usd,
                    pool.raw_json
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    // get_all_raw 修改：读取时也把 tvl_usd 读出来（虽然清洗时主要看 id）
    pub fn get_all_raw(&self) -> Result<Vec<UnifiedPool>> {
        let mut stmt = self.conn.prepare("SELECT id, protocol, token0, token1, tvl_usd, raw_json FROM raw_pools")?;
        let rows = stmt.query_map([], |row| {
            Ok(UnifiedPool {
                id: row.get(0)?,
                protocol: row.get(1)?,
                token0_id: row.get(2)?,
                token0_symbol: "".to_string(),
                token1_id: row.get(3)?,
                token1_symbol: "".to_string(),
                fee: 0,
                tvl_usd: row.get(4)?,
                raw_json: row.get(5)?,
                extra_data: "".to_string(),
            })
        })?;
        
        let mut pools = Vec::new();
        for pool in rows {
            pools.push(pool?);
        }
        Ok(pools)
    }

    pub fn insert_target(&mut self, pool: &UnifiedPool) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO target_pools (address, protocol, token0, token1, fee, extra_data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![pool.id, pool.protocol, pool.token0_id, pool.token1_id, pool.fee, pool.extra_data],
        )?;
        Ok(())
    }
    
    // 清空旧的目标数据（每次全量更新时可能需要）
    pub fn clear_target(&self) -> Result<()> {
        self.conn.execute("DELETE FROM target_pools", [])?;
        Ok(())
    }
}