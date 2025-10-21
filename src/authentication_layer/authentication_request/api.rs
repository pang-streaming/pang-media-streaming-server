use reqwest::Client;
use crate::{authentication_layer::authentication_request::response::{BaseStreamUserResponse}, config::ApiConfig};

pub async fn get_authentication(stream_key: &str, client: &Client, api_config: &ApiConfig) -> Result<BaseStreamUserResponse, String> {
    let data = client
        .post(format!("{}/stream", api_config.host))
        .header("X-Stream-Key", stream_key)
        .send().await
        .unwrap();
    if data.status().is_success() {
        Ok(data.json().await.unwrap())
    } else {
        Err("stream key is not allowed".to_string())
    }
}

pub async fn stop_authentication(stream_key: &str, client: &Client, api_config: &ApiConfig) -> Result<BaseStreamUserResponse, String> {
    let data = client
        .post(format!("{}/stream", api_config.host))
        .header("X-Stream-Key", stream_key)
        .send().await
        .unwrap();
    if data.status().is_success() {
        Ok(data.json().await.unwrap())
    } else {
        Err("stream key is not allowed".to_string())
    }
}