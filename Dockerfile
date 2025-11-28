# 최신 Rust 버전 사용
FROM rust:latest

# FFmpeg, AWS CLI 및 필요한 라이브러리 설치
RUN apt-get update && apt-get install -y \
    ffmpeg \
    pkg-config \
    libssl-dev \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# AWS CLI v2 설치
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# 작업 디렉토리 설정
WORKDIR /app

# 소스 코드 복사
COPY . .

# 애플리케이션 빌드
RUN cargo build --release

EXPOSE 1935

# HLS 출력 디렉토리 생성
RUN mkdir -p /app/hls_output

# 환경 변수 설정
ENV RUST_LOG=debug
# AWS CLI는 환경변수에서 자격증명을 자동으로 읽음:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

# 애플리케이션 실행
CMD ["./target/release/pang-streaming-server"]