/*
 인증 요청 레이어 (authentication_request_layer)
 요청을 GET 방식으로 헤더에 stream key 를 넣어 보내고, 요청에 성공한다면
 username, start_time 을 반환하게 된다.
 */
pub mod api;
pub mod response;