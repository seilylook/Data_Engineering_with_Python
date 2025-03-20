# PostgreSQL 사용자와 데이터베이스 관계

현재 설정:

1. 사용자(User): `airflow`

    - PostgreSQL의 주 사용자로 설정: docker-compose에서 설정

    - 모든 데이터베이스에 대한 접근 권한 보유

2. 데이터베이스(Database):

    - `airflow`: 기본 데이터베이스, Airflow 애플리케이션이 사용

    - `dataengineering`: 추가 데이터베이스, 데이터 엔지니어링 작업에 사용

3. 권한 관계:

    - `airflow` 사용자는 `airflow` 데이터베이스 소유자

    - `airflow` 사용자는 `dataengineering` 데이터베이스에 대한 모든 권한 보유

    - `init_postgresql.sh` 스크립트는 `dataengineering` 데이터베이스의 모든 테이블과 시퀀스에 대한 권한을 `airflow` 사용자에게 부여

# PostgreSQL 데이터 베이스 접근 및 데이터 확인 방법

## Docker CLI를 통한 접근 

```bash
# PostgreSQL 컨테이너에 접속
docker exec -it postgres bash

# PostgreSQL 클라이언트 실행 (airflow 데이터베이스)
psql -U airflow -d airflow

# PostgreSQL 클라이언트 실행 (dataengineering 데이터베이스)
psql -U airflow -d dataengineering
```

## PostgreSQL 클라이언트 내에서 유용한 명령어:

```bash
-- 데이터베이스 목록 확인
\l

-- 현재 연결된 데이터베이스 변경
\c dataengineering

-- 테이블 목록 확인
\dt

-- 특정 테이블 구조 확인
\d users

-- 테이블 데이터 조회
SELECT * FROM users;

-- 사용자 목록 확인
\du

-- PostgreSQL 클라이언트 종료
\q
```

## 직접 명령어 실행 (컨테이너 외부에서)

```bash
# 데이터베이스 목록 확인
docker exec -i postgres psql -U airflow -c "\l"

# 테이블 목록 확인 (dataengineering 데이터베이스)
docker exec -i postgres psql -U airflow -d dataengineering -c "\dt"

# 특정 테이블의 데이터 조회
docker exec -i postgres psql -U airflow -d dataengineering -c "SELECT * FROM users;"

# SQL 파일 실행
docker exec -i postgres psql -U airflow -d dataengineering < your_query.sql
```

## NiFi를 통한 데이터 접근

NiFi UI에서:

1. DBCPConnectionPool 서비스 구성:

    - Database Driver Location: /opt/nifi/nifi-current/lib/custom-drivers/postgresql-42.7.1.jar
    - Database Connection URL: jdbc:postgresql://postgres:5432/dataengineering
    - Database Driver Class Name: org.postgresql.Driver
    - Database User: airflow
    - Password: airflow


2. 데이터 조회를 위한 프로세서 설정:

    - QueryDatabaseTable 또는 ExecuteSQL 프로세서 사용
    - DBCPConnectionPool 서비스 연결
    - SQL 쿼리 설정 (예: SELECT * FROM users;)

## 데이터베이스 GUI 클라이언트 사용
외부 GUI 클라이언트(DBeaver, pgAdmin 등)를 사용하여 접근:

1. 연결 정보:

    - 호스트: localhost
    - 포트: 5432 (docker-compose에서 노출된 포트)
    - 사용자: airflow
    - 비밀번호: airflow
    - 데이터베이스: airflow 또는 dataengineering

## 백업 및 복원

데이터 백업:

```bash
# 특정 테이블 백업
docker exec -i postgres pg_dump -U airflow -d dataengineering -t users > users_backup.sql

# 전체 데이터베이스 백업
docker exec -i postgres pg_dump -U airflow -d dataengineering > dataengineering_backup.sql
```

데이터 복원:

```bash
# 백업 파일에서 복원
cat dataengineering_backup.sql | docker exec -i postgres psql -U airflow -d dataengineering - 
```

# 정리

1. airflow 데이터베이스:

    - docker-compose 파일에서 PostgreSQL 컨테이너 설정 시 POSTGRES_DB: airflow로 지정했기 때문에 컨테이너 시작 시 자동으로 생성됩니다.
    - 이 데이터베이스는 Airflow 서비스(웹서버, 스케줄러, 워커 등)의 메타데이터를 저장하는 데 사용됩니다.
    - Airflow의 작업 기록, DAG 정보, 변수, 연결 정보 등이 이 데이터베이스에 저장됩니다.
    - docker-compose 파일의 Airflow 서비스들은 AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow 설정을 통해 이 데이터베이스에 연결됩니다.


2. dataengineering 데이터베이스:

    - 이 데이터베이스는 실제 데이터 처리 작업을 위한 사용자 정의 데이터베이스입니다.
    - make build 시 init_postgresql.sh를 실행하다록 설정했습니다.
    - init_postgresql.sh 스크립트를 통해 테이블 구조(users 테이블 등)를 생성하고 권한을 설정합니다.
    - NiFi 및 기타 데이터 처리 도구가 이 데이터베이스에 연결하여 실제 데이터를 처리합니다.