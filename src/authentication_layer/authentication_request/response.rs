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
    nickname: String,
    #[serde(rename = "createdAt")]
    created_at: String
}

impl StreamUserResponse {
    pub fn get_nickname(&self) -> String {
        self.nickname.clone()
    }

    pub fn get_start_time(&self) -> String {
        self.created_at.clone()
    }
}