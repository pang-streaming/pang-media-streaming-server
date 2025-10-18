use reqwest::Client;
use scuffle_rtmp::session::server::ServerSessionError;
use crate::authentication_layer::authentication_request::api::get_authentication;
use crate::authentication_layer::authentication_request::response::StreamUserResponse;

pub async fn authenticate_and_get_stream_id(stream_key: &str, client: &Client) -> Result<String, ServerSessionError> {
    let response: StreamUserResponse = get_authentication(stream_key, client).await
        .expect("Authentication failed")
        .data;
    Ok(format!("{}/{}", response.get_nickname(), response.get_start_time()))
}