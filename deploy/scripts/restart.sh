#!/bin/bash

# 스트리밍 서버 재시작 스크립트

echo "Restarting Pang Streaming Server with SRS..."

# 서비스 중지
docker-compose down

# 잠시 대기
sleep 2

# 서비스 시작
docker-compose up -d

# 서비스 상태 확인
echo "Checking service status..."
docker-compose ps

echo "Services restarted successfully!"


