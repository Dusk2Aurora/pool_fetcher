use serde::{Deserialize, Serialize};
use clap::ValueEnum;

// 增加 Clone, ValueEnum 用于命令行参数
#[derive(Debug, Clone, Serialize, Deserialize, ValueEnum, PartialEq)]
pub enum Protocol {
    #[value(name = "v3")]
    UniV3,
    #[value(name = "aerodrome")]
    AerodromeV3,
    #[value(name = "v2")]
    UniV2,
    // 新增 V4
    #[value(name = "v4")]
    UniV4,
    // 新增 Pancake V4
    #[value(name = "pancake_v4")]
    PancakeV4,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::UniV3 => "V3",
            Protocol::AerodromeV3 => "Aerodrome-V3",
            Protocol::UniV2 => "V2",
            Protocol::UniV4 => "V4",
            Protocol::PancakeV4 => "Pancake-V4",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedPool {
    pub id: String,
    pub protocol: String,
    pub token0_id: String,
    pub token0_symbol: String,
    pub token1_id: String,
    pub token1_symbol: String,
    pub fee: u32,
    pub decimals0: Option<u8>,
    pub decimals1: Option<u8>,
    pub raw_json: String,
    pub extra_data: String,
    pub tvl_usd: f64,
}

#[derive(Deserialize, Debug)]
pub struct GraphResponse<T> {
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPart {
    pub id: String,
    pub symbol: String,
    /// Subgraph 返回的 decimals 是字符串（BigInt），用 Option 兜底缺失情况
    #[serde(default)]
    pub decimals: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3Pool {
    pub id: String,
    pub token0: TokenPart,
    pub token1: TokenPart,
    #[serde(default)]
    pub feeTier: String, 
    #[serde(default)]
    pub liquidity: String,
    #[serde(default)]
    pub totalValueLockedUSD: String, 
}

#[derive(Deserialize, Debug)]
pub struct V3Data {
    pub pools: Vec<V3Pool>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4Pool {
    pub id: String,
    pub token0: TokenPart,
    pub token1: TokenPart,
    #[serde(default)]
    pub feeTier: String,
    #[serde(default)]
    pub hooks: String,
    #[serde(default)]
    pub liquidity: String,
    #[serde(default)]
    pub totalValueLockedUSD: String,
}

#[derive(Deserialize, Debug)]
pub struct V4Data {
    pub pools: Vec<V4Pool>,
}