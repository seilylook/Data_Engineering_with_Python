#!/bin/bash

# PostgreSQL 컨테이너가 실행 중인지 확인
if ! docker ps | grep -q "postgres"; then
    echo "PostgreSQL 컨테이너가 실행되고 있지 않습니다."
    exit 1
fi

# 먼저 데이터베이스 존재 여부 확인 및 생성
echo "dataengineering 데이터베이스 생성 중..."
docker exec -i postgres bash -c 'PGPASSWORD=airflow psql -U airflow -d airflow -c "CREATE DATABASE dataengineering;"'

# 이제 dataengineering 데이터베이스에 테이블 생성
cat << 'EOF' > /tmp/create_tables.sql
-- users 테이블이 존재하지 않으면 생성
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    street VARCHAR(200),
    city VARCHAR(100),
    zip VARCHAR(10),
    lng DECIMAL(10, 6),
    lat DECIMAL(10, 6)
);

-- 테이블이 성공적으로 생성되었는지 확인
\dt
-- 테이블 구조 확인
\d users
EOF

# PostgreSQL 컨테이너에 SQL 파일 복사
docker cp /tmp/create_tables.sql postgres:/tmp/create_tables.sql

# PostgreSQL 컨테이너 내에서 SQL 스크립트 실행 (dataengineering 데이터베이스에 직접 연결)
echo "테이블 생성 중..."
docker exec -i postgres bash -c 'PGPASSWORD=airflow psql -U airflow -d dataengineering -f /tmp/create_tables.sql'

# 권한 부여
echo "권한 부여 중..."
docker exec -i postgres bash -c 'PGPASSWORD=airflow psql -U airflow -d dataengineering -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow; GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;"'

# 실행 결과 확인
if [ $? -eq 0 ]; then
    echo "데이터베이스와 테이블이 성공적으로 생성되었습니다."
    # 임시 파일 삭제
    rm /tmp/create_tables.sql
    docker exec postgres rm /tmp/create_tables.sql
else
    echo "오류가 발생했습니다."
    # 임시 파일 삭제
    rm /tmp/create_tables.sql
    docker exec postgres rm /tmp/create_tables.sql
    exit 1
fi

# 데이터베이스 연결 테스트
echo "데이터베이스 연결 테스트 중..."
docker exec postgres pg_isready -U airflow -d dataengineering

if [ $? -eq 0 ]; then
    echo "데이터베이스가 정상적으로 응답합니다."
else
    echo "데이터베이스 연결 테스트에 실패했습니다."
    exit 1
fi