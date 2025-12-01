use reqwest::Client;
use scuffle_rtmp::session::server::ServerSessionError;
use crate::{authentication_layer::authentication_request::{api::get_authentication, response::StreamUserResponse}, config::ApiConfig};
use crate::authentication_layer::authentication_request::api::stop_authentication;

pub async fn authenticate_and_get_stream_id(stream_key: &str, client: &Client, api_config: &ApiConfig) -> Result<String, ServerSessionError> {
    let response: StreamUserResponse = get_authentication(stream_key, client, api_config).await
        .expect("Authentication failed")
        .data;
    Ok(format!("{}/{}", response.get_username(), response.get_start_time()))
}

pub async fn stop_stream(stream_key: &str, client: &Client, api_config: &ApiConfig) {
    stop_authentication(stream_key, client, api_config).await
        .expect("Send Failed");
    return;
}