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
    pub id: String,         // 池子地址 (0x...)
    pub protocol: String,   // V3, V2, Aerodrome-V3
    pub token0_id: String,  // Token0 地址
    pub token0_symbol: String,
    pub token1_id: String,  // Token1 地址
    pub token1_symbol: String,
    pub fee: u32,           // V3 用 feeTier, V2 默认为 3000 (0.3%)
    pub raw_json: String,   // 原始 JSON 数据，用于备份
    pub extra_data: String, // 存储 liquidity, TVL 等额外参考数据
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
    pub feeTier: String, // 有些 subgraph 返回 string
    #[serde(default)]
    pub liquidity: String,
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