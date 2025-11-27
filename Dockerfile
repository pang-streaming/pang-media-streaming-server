# 최신 Rust 버전 사용
FROM rust:latest

# FFmpeg 및 필요한 라이브러리 설치
RUN apt-get update && apt-get install -y \
    ffmpeg \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# 소스 코드 복사
COPY . .

# 애플리케이션 빌드
RUN cargo build --release

EXPOSE 1935

# HLS 출력 디렉토리 생성
RUN mkdir -p /app/hls_output

# Rust 로그 환경 변수 설정
ENV RUST_LOG=debug

# 애플리케이션 실행
CMD ["./target/release/pang-streaming-server"]