#!/bin/bash

# 스트리밍 서버 시작 스크립트

echo "Starting Pang Streaming Server with SRS..."

# 로그 디렉토리 생성
mkdir -p /app/logs

# 권한 설정
chmod 755 /app/logs

# 서비스 시작
echo "Starting services..."
docker-compose up -d

# 서비스 상태 확인
echo "Checking service status..."
docker-compose ps

echo "Services started successfully!"
echo "RTMP URL: rtmp://localhost:1935/stream"
echo "HLS URL: http://localhost:8080/hls/"
echo "SRS API URL: http://localhost:1985"
echo "WebRTC URL: http://localhost:8080/rtc/"


