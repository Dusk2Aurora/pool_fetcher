use rusqlite::{params, Connection, Result};
use crate::models::UnifiedPool;

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self { conn })
    }

    pub fn init(&self) -> Result<()> {
        // 1. 粗数据表：存储所有抓取到的数据
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_pools (
                id TEXT PRIMARY KEY,
                protocol TEXT NOT NULL,
                token0 TEXT NOT NULL,
                token1 TEXT NOT NULL,
                raw_json TEXT
            )",
            [],
        )?;

        // 2. 目标表：主程序使用的清洗后的表
        // 包含 fee 和 extra_data
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

    pub fn insert_raw(&mut self, pool: &UnifiedPool) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO raw_pools (id, protocol, token0, token1, raw_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![pool.id, pool.protocol, pool.token0_id, pool.token1_id, pool.raw_json],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// 获取所有原始数据用于清洗
    pub fn get_all_raw(&self) -> Result<Vec<UnifiedPool>> {
        let mut stmt = self.conn.prepare("SELECT id, protocol, token0, token1, raw_json FROM raw_pools")?;
        let rows = stmt.query_map([], |row| {
            let raw_json: String = row.get(4)?;
            // 这里我们需要反序列化 raw_json 来恢复完整结构，或者简化处理
            // 为了简化，我们在清洗阶段主要用到 id, protocol, token0, token1
            // 完整数据如果在 raw_json 里，可以再次解析
            Ok(UnifiedPool {
                id: row.get(0)?,
                protocol: row.get(1)?,
                token0_id: row.get(2)?,
                token0_symbol: "".to_string(), // 暂时不需要
                token1_id: row.get(3)?,
                token1_symbol: "".to_string(), // 暂时不需要
                fee: 0, 
                raw_json,
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