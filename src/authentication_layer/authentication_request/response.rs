use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct BaseStreamUserResponse {
    #[serde(rename = "status")]
    _status: String,
    #[serde(rename = "message")]
    _message: String,
    pub(crate) data: StreamUserResponse,
    #[serde(rename = "timestamp")]
    _timestamp: String
}

#[derive(Deserialize, Debug)]
pub struct StreamUserResponse {
    username: String,
    #[serde(rename = "startAt")]
    start_at: String,
}

impl StreamUserResponse {
    pub fn get_username(&self) -> String {
        self.username.clone()
    }

    pub fn get_start_time(&self) -> String {
        self.start_at.clone()
    }
}