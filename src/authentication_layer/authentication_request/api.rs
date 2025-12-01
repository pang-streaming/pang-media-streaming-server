use reqwest::Client;
use serde::Serialize;
use log::{info, error};
use crate::{authentication_layer::authentication_request::response::{BaseStreamUserResponse}, config::ApiConfig};

#[derive(Serialize)]
struct StreamEventPayload<'a> {
    stream_key: &'a str,
    stream_id: &'a str,
}

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
        .delete(format!("{}/stream", api_config.host))
        .header("X-Stream-Key", stream_key)
        .send().await
        .unwrap();
    if data.status().is_success() {
        Ok(data.json().await.unwrap())
    } else {
        Err("stream key is not allowed".to_string())
    }
}