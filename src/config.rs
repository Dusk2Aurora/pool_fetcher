use std::env;
use dotenv::dotenv;
use anyhow::Result;

pub struct Config {
    pub db_path: String,
    pub ticks_db_path: String,
    pub url_uni_v3: String,
    pub url_aerodrome: String,
    pub url_uni_v2: String,
    pub url_uni_v4: String,
    pub url_pancake_v4: String,
}

impl Config {
    pub fn load() -> Result<Self> {
        dotenv().ok();

        let db_path = env::var("DATABASE_URL").unwrap_or_else(|_| "pools.db".to_string());
        let ticks_db_path = env::var("TICKS_DATABASE_URL").unwrap_or_else(|_| "ticks.db".to_string());
        
        let url_uni_v3 = env::var("URL_UNIV3_BASE")
            .expect("Missing URL_UNIV3_BASE in .env");
        let url_aerodrome = env::var("URL_AERODROME_BASE")
            .expect("Missing URL_AERODROME_BASE in .env");
        let url_uni_v2 = env::var("URL_UNIV2_BASE")
            .expect("Missing URL_UNIV2_BASE in .env");
        // 读取 V4 URL
        let url_uni_v4 = env::var("URL_UNIV4_BASE")
            .expect("Missing URL_UNIV4_BASE in .env");
        
        let url_pancake_v4 = env::var("URL_PANCAKE_V4_BASE")
            .expect("Missing URL_PANCAKE_V4_BASE in .env");

        Ok(Self {
            db_path,
            ticks_db_path,
            url_uni_v3,
            url_aerodrome,
            url_uni_v2,
            url_uni_v4,
            url_pancake_v4,
        })
    }
}