use std::env;
use dotenv::dotenv;
use anyhow::Result;

pub struct Config {
    pub db_path: String,
    pub url_uni_v3: String,
    pub url_aerodrome: String,
    pub url_uni_v2: String,
}

impl Config {
    pub fn load() -> Result<Self> {
        dotenv().ok();

        let db_path = env::var("DATABASE_URL").unwrap_or_else(|_| "pools.db".to_string());
        
        let url_uni_v3 = env::var("URL_UNIV3_BASE")
            .expect("Missing URL_UNIV3_BASE in .env");
        let url_aerodrome = env::var("URL_AERODROME_BASE")
            .expect("Missing URL_AERODROME_BASE in .env");
        let url_uni_v2 = env::var("URL_UNIV2_BASE")
            .expect("Missing URL_UNIV2_BASE in .env");

        Ok(Self {
            db_path,
            url_uni_v3,
            url_aerodrome,
            url_uni_v2,
        })
    }
}