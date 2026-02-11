use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Protocol {
    UniV3,
    AerodromeV3,
    UniV2,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::UniV3 => "V3",
            Protocol::AerodromeV3 => "Aerodrome-V3",
            Protocol::UniV2 => "V2",
        }
    }
}

/// 统一的池子结构，用于存入数据库
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedPool {
    pub id: String,
    pub protocol: String,
    pub token0_id: String,
    pub token0_symbol: String,
    pub token1_id: String,
    pub token1_symbol: String,
    pub fee: u32,
    pub raw_json: String,
    pub extra_data: String,
    pub tvl_usd: f64, // 新增：用于存储 TVL
}

// 以下是用于解析 GraphQL 响应的辅助结构

#[derive(Deserialize, Debug)]
pub struct GraphResponse<T> {
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPart {
    pub id: String,
    pub symbol: String,
}

// V3 风格的响应结构 (UniV3, Aerodrome)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3Pool {
    pub id: String,
    pub token0: TokenPart,
    pub token1: TokenPart,
    #[serde(default)]
    pub feeTier: String,
    #[serde(default)]
    pub liquidity: String,
    #[serde(default)] // 默认为 "0" 防止字段不存在报错
    pub totalValueLockedUSD: String, 
}


#[derive(Deserialize, Debug)]
pub struct V3Data {
    pub pools: Vec<V3Pool>,
}

// V2 风格的响应结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2Pair {
    pub id: String,
    pub token0: TokenPart,
    pub token1: TokenPart,
    #[serde(default)]
    pub reserveUSD: String,
}

#[derive(Deserialize, Debug)]
pub struct V2Data {
    pub pairs: Vec<V2Pair>,
}