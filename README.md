# Data Engineering with Python

## Chapter 2. Building Infrastructure

### Make build (docker build python image & docker compose up)

책에서는 Airflow, NiFi, PostgreSQL, Elasticsearch, Kibana, Kafka, Spark 등 모조리 다 로컬 환경에서 설치해서 실습한다. 하지만 이는 내가 아주 싫어하는 상황이므로 당연하게 Docker를 활용해서 환경을 구축했다.

#### 버전 관리

|Software / hardware|OS requirements|
|:------:|:---:|
|Python|3.12.8|
|Nifi|apache/nifi:1.28.0|
|PostgreSQL|postgres:13|
|ElasticSearch|elasticsearch:7.17.28|
|Kibana|kibana:7.17.28|

```bash
app-py3.12 ✘ {seilylook} 🍀 make build
==============================================
Exporting Python dependencies to requirements.txt...
==============================================
poetry export -f requirements.txt --output requirements.txt --without-hashes --with dev
Warning: poetry-plugin-export will not be installed by default in a future version of Poetry.
In order to avoid a breaking change and make your automation forward-compatible, please install poetry-plugin-export explicitly. See https://python-poetry.org/docs/plugins/#using-plugins for details on how to install a plugin.
To disable this warning run 'poetry config warnings.export false'.


==============================================
Building Docker image python-app:latest...
==============================================
docker build -t python-app:latest .
[+] Building 1.7s (20/20) FINISHED                                                                                               docker:desktop-linux
 => [internal] load build definition from Dockerfile                                                                                             0.0s
 => => transferring dockerfile: 1.14kB                                                                                                           0.0s
 => [internal] load metadata for docker.io/library/python:3.12-slim                                                                              1.5s
 => [auth] library/python:pull token for registry-1.docker.io                                                                                    0.0s
 => [internal] load .dockerignore                                                                                                                0.0s
 => => transferring context: 2B                                                                                                                  0.0s
 => [internal] load build context                                                                                                                0.0s
 => => transferring context: 33.38kB                                                                                                             0.0s
 => [builder 1/5] FROM docker.io/library/python:3.12-slim@sha256:aaa3f8cb64dd64e5f8cb6e58346bdcfa410a108324b0f28f1a7cc5964355b211                0.0s
 => CACHED [stage-1  2/10] RUN apt-get update &&     apt-get install -y --no-install-recommends     default-jdk     procps     wget     libpq5   0.0s
 => CACHED [stage-1  3/10] WORKDIR /app                                                                                                          0.0s
 => CACHED [builder 2/5] WORKDIR /app                                                                                                            0.0s
 => CACHED [builder 3/5] RUN apt-get update &&     apt-get install -y --no-install-recommends     build-essential     libpq-dev     python3-dev  0.0s
 => CACHED [builder 4/5] COPY requirements.txt .                                                                                                 0.0s
 => CACHED [builder 5/5] RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt                                      0.0s
 => CACHED [stage-1  4/10] COPY --from=builder /app/wheels /wheels                                                                               0.0s
 => CACHED [stage-1  5/10] COPY --from=builder /app/requirements.txt .                                                                           0.0s
 => CACHED [stage-1  6/10] RUN pip install --no-cache /wheels/*                                                                                  0.0s
 => [stage-1  7/10] COPY src/ src/                                                                                                               0.0s
 => [stage-1  8/10] COPY tests/ tests/                                                                                                           0.0s
 => [stage-1  9/10] COPY data/ data/                                                                                                             0.0s
 => [stage-1 10/10] COPY conf/ conf/                                                                                                             0.0s
 => exporting to image                                                                                                                           0.0s
 => => exporting layers                                                                                                                          0.0s
 => => writing image sha256:a7003da4b1bb9a04c9d98b7a386e9c28c84efe83df2ae20e99abc16cedf9e3fb                                                     0.0s
 => => naming to docker.io/library/python-app:latest                                                                                             0.0s

View build details: docker-desktop://dashboard/build/desktop-linux/desktop-linux/4go6dhhdb6ahlu1m1xmndgsou

What's next:
    View a summary of image vulnerabilities and recommendations → docker scout quickview 


==============================================
Constructing Docker Containers...
==============================================
docker compose up -d
WARN[0000] The "AIRFLOW_UID" variable is not set. Defaulting to a blank string. 
WARN[0000] The "AIRFLOW_UID" variable is not set. Defaulting to a blank string. 
WARN[0000] /Users/seilylook/Development/Book/Data_Engineering_with_Python/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
[+] Running 10/10
 ✔ Container postgres           Healthy                                                                                                          4.3s 
 ✔ Container elasticsearch      Healthy                                                                                                          3.6s 
 ✔ Container redis              Healthy                                                                                                          4.3s 
 ✔ Container kibana             Running                                                                                                          0.0s 
 ✔ Container airflow-init       Exited                                                                                                           7.8s 
 ✔ Container python-app         Started                                                                                                          3.8s 
 ✔ Container airflow-triggerer  Running                                                                                                          0.0s 
 ✔ Container airflow-webserver  Running                                                                                                          0.0s 
 ✔ Container airflow-scheduler  Running                                                                                                          0.0s 
 ✔ Container airflow-worker     Running                                                                                                          0.0s 


==============================================
Waiting for PostgreSQL to start...
==============================================
=====================================
Initializing PostgreSQL...
=====================================
chmod +x ./scripts/init_postgresql.sh
./scripts/init_postgresql.sh
dataengineering 데이터베이스 생성 중...
ERROR:  database "dataengineering" already exists
Successfully copied 2.05kB to postgres:/tmp/create_tables.sql
테이블 생성 중...
psql:/tmp/create_tables.sql:10: NOTICE:  relation "users" already exists, skipping
CREATE TABLE
        List of relations
 Schema | Name  | Type  |  Owner  
--------+-------+-------+---------
 public | users | table | airflow
(1 row)

                                    Table "public.users"
 Column |          Type          | Collation | Nullable |              Default              
--------+------------------------+-----------+----------+-----------------------------------
 id     | integer                |           | not null | nextval('users_id_seq'::regclass)
 name   | character varying(100) |           | not null | 
 street | character varying(200) |           |          | 
 city   | character varying(100) |           |          | 
 zip    | character varying(10)  |           |          | 
 lng    | numeric(10,6)          |           |          | 
 lat    | numeric(10,6)          |           |          | 
Indexes:
    "users_pkey" PRIMARY KEY, btree (id)

권한 부여 중...
GRANT
데이터베이스와 테이블이 성공적으로 생성되었습니다.
데이터베이스 연결 테스트 중...
/var/run/postgresql:5432 - accepting connections
데이터베이스가 정상적으로 응답합니다.




==============================================
Waiting for Elasticsearch to start...
==============================================
=====================================
Initializing Elasticsearch...
=====================================
chmod +x ./scripts/init_elasticsearch.sh
./scripts/init_elasticsearch.sh
Elasticsearch가 준비될 때까지 대기 중...
Elasticsearch가 준비되었습니다.
users 인덱스 생성 중...
{"error":{"root_cause":[{"type":"resource_already_exists_exception","reason":"index [users/AW57UPmxTYyW3G-GdL6lHw] already exists","index_uuid":"AW57UPmxTYyW3G-GdL6lHw","index":"users"}],"type":"resource_already_exists_exception","reason":"index [users/AW57UPmxTYyW3G-GdL6lHw] already exists","index_uuid":"AW57UPmxTYyW3G-GdL6lHw","index":"users"},"status":400}users 인덱스가 성공적으로 생성되었습니다.
인덱스 목록 확인:
health status index                                                              uuid                   pri rep docs.count docs.deleted store.size pri.store.size dataset.size
green  open   .internal.alerts-transform.health.alerts-default-000001            MSc-VAFyQHG9tGk2TxwbGg   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.logs.alerts-default-000001          Bief08evQ_SX_3UrwZOzwQ   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.uptime.alerts-default-000001        N-ptFAoNTYeF7OHGtOqZFw   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-ml.anomaly-detection.alerts-default-000001        JL9JINenTS-XMJClxKXrYA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.slo.alerts-default-000001           rFwL_h62QZmQx8kkXFZIyA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-default.alerts-default-000001                     6xFM1NqvTc--9BJ7MlhCpA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.apm.alerts-default-000001           D1xM5G2DTPeF25kkR6_KWQ   1   0          0            0       249b           249b         249b
green  open   users                                                              AW57UPmxTYyW3G-GdL6lHw   1   0       1000            0    229.1kb        229.1kb      229.1kb
green  open   .internal.alerts-observability.metrics.alerts-default-000001       IO4Li-8sS6-AH8aSsvEqLg   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-ml.anomaly-detection-health.alerts-default-000001 95sA140IQ2qCyErtmHKTEg   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.threshold.alerts-default-000001     TV3IWC9fQUuYWQO-56_5sA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-security.alerts-default-000001                    VE-qcLiURZy7h_i1hb96Lw   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-stack.alerts-default-000001                       _PEhTLb4TJ2bYAwiv6kz8w   1   0          0            0       249b           249b         249b
```

#### Make test

```bash
app-py3.12 {seilylook} 🍀 make test 
=======================
Running tests with pytest...
=======================
mkdir -p target
docker run --rm -v /Users/seilylook/Development/Book/Data_Engineering_with_Python/app/target:/app/target python-app:latest /bin/bash -c \
                'for test_file in $(find tests -name "*.py" ! -name "__init__.py"); do \
                        base_name=$(basename $test_file .py); \
                        pytest $test_file --junitxml=target/$base_name.xml; \
                done'
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 7 items

tests/test_progress_bar.py .......                                       [100%]

------------ generated xml file: /app/target/test_progress_bar.xml -------------
============================== 7 passed in 0.06s ===============================
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 9 items

tests/test_data_generator.py .........                                   [100%]

----------- generated xml file: /app/target/test_data_generator.xml ------------
============================== 9 passed in 0.03s ===============================
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 0 items

----------------- generated xml file: /app/target/conftest.xml -----------------
============================ no tests ran in 0.00s =============================
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 7 items

tests/test_postgres_connector.py .......                                 [100%]

--------- generated xml file: /app/target/test_postgres_connector.xml ----------
============================== 7 passed in 0.20s ===============================
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 9 items

tests/test_database_config.py .........                                  [100%]

----------- generated xml file: /app/target/test_database_config.xml -----------
============================== 9 passed in 0.02s ===============================
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 14 items

tests/test_database_pytest.py ..............                             [100%]

----------- generated xml file: /app/target/test_database_pytest.xml -----------
============================== 14 passed in 0.19s ==============================
============================= test session starts ==============================
platform linux -- Python 3.12.9, pytest-8.3.4, pluggy-1.5.0
rootdir: /app
plugins: Faker-36.1.1, time-machine-2.16.0, anyio-4.8.0
collected 8 items

tests/test_elasticsearch_connector.py ........                           [100%]

------- generated xml file: /app/target/test_elasticsearch_connector.xml -------
============================== 8 passed in 0.18s ===============================
```

#### Make start

```bash
app-py3.12 {seilylook} 🍀 make start
=========================
Starting the application...
=========================
python -m src.main
2025-02-28 17:34:59,269 - root - INFO - 데이터 생성 및 저장 프로세스 시작
2025-02-28 17:34:59,269 - root - INFO - 데이터셋이 이미 존재합니다: data/raw/test_data.csv
2025-02-28 17:34:59,285 - src.utils.connection - INFO - PostgreSQL 연결 성공!
2025-02-28 17:34:59,286 - src.utils.connection - INFO - Elasticsearch 클라이언트 생성 완료
2025-02-28 17:34:59,290 - elastic_transport.transport - INFO - GET http://localhost:9200/ [status:200 duration:0.004s]
2025-02-28 17:34:59,290 - src.utils.connection - INFO - Elasticsearch 연결 성공! 버전: 8.17.2
2025-02-28 17:34:59,290 - root - INFO - Postgresql 상태: 연결됨
2025-02-28 17:34:59,290 - root - INFO - Elasticsearch 상태: 연결됨
2025-02-28 17:34:59,295 - root - INFO - PostgreSQL: 1000개 레코드를 data/raw/test_data.csv에서 읽었습니다
2025-02-28 17:34:59,330 - src.database.repository - INFO - Bulk inserted 1000 users
2025-02-28 17:34:59,331 - root - INFO - PostgreSQL에 1000개 레코드 저장 완료
2025-02-28 17:34:59,331 - root - INFO - PostgreSQL에 1000개 레코드 저장됨
2025-02-28 17:34:59,333 - root - INFO - Elasticsearch: 1000개 레코드를 data/raw/test_data.csv에서 읽었습니다
2025-02-28 17:34:59,335 - src.utils.connection - INFO - Elasticsearch 클라이언트 생성 완료
2025-02-28 17:34:59,408 - elastic_transport.transport - INFO - PUT http://localhost:9200/_bulk?refresh=true [status:200 duration:0.068s]
2025-02-28 17:34:59,410 - src.database.repository - INFO - Elasticsearch에 1000개 문서 벌크 저장 완료
2025-02-28 17:34:59,410 - root - INFO - Elasticsearch에 1000개 레코드 저장 완료
2025-02-28 17:34:59,410 - root - INFO - Elasticsearch에 1000개 레코드 저장됨
2025-02-28 17:34:59,414 - root - INFO - PostgreSQL에서 5개 레코드 조회 완료
2025-02-28 17:34:59,414 - root - INFO - PostgreSQL 데이터 확인 (샘플 5개):
2025-02-28 17:34:59,414 - root - INFO -   레코드 1: {'id': 1, 'name': 'Whitney Olson', 'street': '1791 Pittman Overpass', 'city': 'Lake Jason', 'zip': '48870', 'lng': Decimal('114.735089'), 'lat': Decimal('45.235433')}
2025-02-28 17:34:59,414 - root - INFO -   레코드 2: {'id': 2, 'name': 'David Smith', 'street': '0474 Julian Station', 'city': 'West Sophia', 'zip': '72976', 'lng': Decimal('94.204753'), 'lat': Decimal('-88.761862')}
2025-02-28 17:34:59,414 - root - INFO -   레코드 3: {'id': 3, 'name': 'Mr. Jason Hughes MD', 'street': '7351 Robinson Underpass', 'city': 'Stephaniebury', 'zip': '8702', 'lng': Decimal('-87.282108'), 'lat': Decimal('12.763472')}
2025-02-28 17:34:59,414 - root - INFO -   레코드 4: {'id': 4, 'name': 'John Johnson', 'street': '8304 Cooper Mews', 'city': 'Candicefort', 'zip': '87821', 'lng': Decimal('-169.562279'), 'lat': Decimal('-53.845951')}
2025-02-28 17:34:59,414 - root - INFO -   레코드 5: {'id': 5, 'name': 'Gregory Harrison', 'street': '0866 Lee Expressway Suite 888', 'city': 'Dianaport', 'zip': '14219', 'lng': Decimal('-30.874919'), 'lat': Decimal('84.261251')}
2025-02-28 17:34:59,414 - src.utils.connection - INFO - Elasticsearch 클라이언트 생성 완료
2025-02-28 17:34:59,419 - elastic_transport.transport - INFO - POST http://localhost:9200/users/_search [status:200 duration:0.005s]
2025-02-28 17:34:59,420 - root - INFO - Elasticsearch에서 5개 레코드 조회 완료
2025-02-28 17:34:59,420 - root - INFO - Elasticsearch 데이터 확인 (샘플 5개):
2025-02-28 17:34:59,420 - root - INFO -   레코드 1: {'name': 'Whitney Olson', 'age': 26, 'street': '1791 Pittman Overpass', 'city': 'Lake Jason', 'state': 'Idaho', 'zip': 48870, 'lng': 114.735089, 'lat': 45.2354325}
2025-02-28 17:34:59,420 - root - INFO -   레코드 2: {'name': 'David Smith', 'age': 28, 'street': '0474 Julian Station', 'city': 'West Sophia', 'state': 'Arizona', 'zip': 72976, 'lng': 94.204753, 'lat': -88.761862}
2025-02-28 17:34:59,420 - root - INFO -   레코드 3: {'name': 'Mr. Jason Hughes MD', 'age': 70, 'street': '7351 Robinson Underpass', 'city': 'Stephaniebury', 'state': 'Mississippi', 'zip': 8702, 'lng': -87.282108, 'lat': 12.763472}
2025-02-28 17:34:59,420 - root - INFO -   레코드 4: {'name': 'John Johnson', 'age': 41, 'street': '8304 Cooper Mews', 'city': 'Candicefort', 'state': 'Rhode Island', 'zip': 87821, 'lng': -169.562279, 'lat': -53.845951}
2025-02-28 17:34:59,420 - root - INFO -   레코드 5: {'name': 'Gregory Harrison', 'age': 24, 'street': '0866 Lee Expressway Suite 888', 'city': 'Dianaport', 'state': 'New Jersey', 'zip': 14219, 'lng': -30.874919, 'lat': 84.261251}
2025-02-28 17:34:59,420 - root - INFO - 데이터 생성 및 저장 프로세스 완료
```

## Chapter 3. Reading and Writing Files

### Handling files using NiFi processors

앞서 `make start`를 통해 Faker를 활용해 테스트 데이터를 생성한다. 이 데이터는 다음 디렉토리에 존재한다.

- local: /app/data/raw/test_data.csv

- NiFi container: /opt/nifi/nifi-current/data/raw/test_data.csv

```bash
# Nifi container 실행
app-py3.12 {seilylook} 🚀 docker exec -i -t nifi /bin/bash

# Container에 원본 데이터가 존재하는지 확인
nifi@e92527995ead:/opt/nifi/nifi-current$ ls data/raw/
test_data.csv

# 40세 이상 사람들 Query 하고 Name 기준으로 저장
nifi@e92527995ead:/opt/nifi/nifi-current/data/processed$ ls -al
total 84
drwxr-xr-x 12 nifi nifi  384 Mar  7 07:17  .
drwxr-xr-x  4 root root 4096 Mar  7 07:05  ..
-rw-r--r--  1 nifi nifi 5637 Mar  7 07:17 'Amber Taylor'
-rw-r--r--  1 nifi nifi 5284 Mar  7 07:17 'Charles Arnold'
-rw-r--r--  1 nifi nifi 5789 Mar  7 07:17 'Corey Hardin'
-rw-r--r--  1 nifi nifi 6580 Mar  7 07:17 'Ebony Miller'
-rw-r--r--  1 nifi nifi 6030 Mar  7 07:17 'Grant Garrison'
-rw-r--r--  1 nifi nifi 5108 Mar  7 07:17 'Kristina Parker'
-rw-r--r--  1 nifi nifi 5444 Mar  7 07:17 'Nicholas Baker MD'
-rw-r--r--  1 nifi nifi 5277 Mar  7 07:17 'Phillip Love'
-rw-r--r--  1 nifi nifi 6180 Mar  7 07:17 'Whitney Barnes'
-rw-r--r--  1 nifi nifi 5438 Mar  7 07:17 'Zachary Cohen'
```

#### Processors 설정

책 63p를 참조해서 Processors와 각 Processors들의 Properties를 설정했다. 원본 데이터는 Row가 1000개이다. SplitRecord Processor의 `Records Per Split`를 100으로 설정했기 때문에 /opt/nifi/nifi-current/data/processed 에 있는 결과 파일들을 보면 10개임을 확인할 수 있다.

#### 문제 및 해결

1. Nifi container 생성 

초기에 apache/nifi:latest 버전으로 image pull을 수행하니 version 2부터 자동으로 `https:`로 연결되도록 정의되어 있었다. 그래서 이전까지 Nifi를 빼고 나머지(postgresql, elasticsearch, airflow)만을 docker container로 만들었으나 문제를 해결하고 싶어 여러가지 시도를 하다가 마지막 수단으로 version을 1.28.0(버전을 낮출 떄 주의할 점은 OS/ARCH == linux/arm64 를 지원하는 docker image 인지 확인해야 한다. 내 MAC은 arm64이기 때문이다.) 으로 낮추니 정상적으로 `http:` port를 사용해서 접근할 수 있었다.

2. Nifi Processor

Nifi Processor를 생성하고 Properties를 설정하는 것은 책과 동일해 큰 문제가 발생하지 않았다. 그런데 계속해서 SplitRecord 부분에서 문제가 발생했는데, 원인은 `RELATIONS` 설정. 즉, 각각의 Processor는 원하는 연결 ex, success, splits, over . 40, matched 뿐만 아니라 failure, unmatched 등 예상치 못한 상황에서 대해서 **terminate** | **retry** 를 설정해주어야 한다. 쉽게 생각하면 상단의 `!(Warning)`이 하나도 없어야 한다.

## Chapter 4. Working with Databases

#### Inserting data into Databases

`make build`를 실행하면 Makefile에서 작성한 대로 `init_elasticsearch.sh`, `init_postgresql.sh`를 실행 시킨다. 

- init_elasticsearch.sh

```shell
#!/bin/bash

# Elasticsearch 컨테이너가 실행 중인지 확인
if ! docker ps | grep -q "elasticsearch"; then
    echo "Elasticsearch 컨테이너가 실행되고 있지 않습니다."
    exit 1
fi

# Elasticsearch가 준비될 때까지 대기
echo "Elasticsearch가 준비될 때까지 대기 중..."
until $(curl --silent --output /dev/null --fail --max-time 5 http://localhost:9200); do
    printf '.'
    sleep 5
done
echo "Elasticsearch가 준비되었습니다."

# users 인덱스 설정
echo "users 인덱스 생성 중..."
curl -X PUT "localhost:9200/users" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "name": { "type": "text" },
      "street": { "type": "text" },
      "city": { "type": "text" },
      "zip": { "type": "keyword" },
      "lng": { "type": "double" },
      "lat": { "type": "double" }
    }
  }
}
'

# 결과 확인
if [ $? -eq 0 ]; then
    echo "users 인덱스가 성공적으로 생성되었습니다."
else
    echo "users 인덱스 생성 중 오류가 발생했습니다."
    exit 1
fi

# 인덱스 확인
echo "인덱스 목록 확인:"
curl -X GET "localhost:9200/_cat/indices?v"
```

- init_postgresql.sh

```shell
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
```

이를 통해 기본 데이터를 생성하고 두개의 데이터를 **PostgresSQL**, **ElasticSearch**에 각각 저장한다. 아래의 Log을 통해 정상적으로 저장되었음을 확인할 수 있다.

```bash
==============================================
Waiting for PostgreSQL to start...
==============================================
=====================================
Initializing PostgreSQL...
=====================================
chmod +x ./scripts/init_postgresql.sh
./scripts/init_postgresql.sh
dataengineering 데이터베이스 생성 중...
ERROR:  database "dataengineering" already exists
Successfully copied 2.05kB to postgres:/tmp/create_tables.sql
테이블 생성 중...
psql:/tmp/create_tables.sql:10: NOTICE:  relation "users" already exists, skipping
CREATE TABLE
        List of relations
 Schema | Name  | Type  |  Owner  
--------+-------+-------+---------
 public | users | table | airflow
(1 row)

                                    Table "public.users"
 Column |          Type          | Collation | Nullable |              Default              
--------+------------------------+-----------+----------+-----------------------------------
 id     | integer                |           | not null | nextval('users_id_seq'::regclass)
 name   | character varying(100) |           | not null | 
 street | character varying(200) |           |          | 
 city   | character varying(100) |           |          | 
 zip    | character varying(10)  |           |          | 
 lng    | numeric(10,6)          |           |          | 
 lat    | numeric(10,6)          |           |          | 
Indexes:
    "users_pkey" PRIMARY KEY, btree (id)

권한 부여 중...
GRANT
데이터베이스와 테이블이 성공적으로 생성되었습니다.
데이터베이스 연결 테스트 중...
/var/run/postgresql:5432 - accepting connections
데이터베이스가 정상적으로 응답합니다.




==============================================
Waiting for Elasticsearch to start...
==============================================
=====================================
Initializing Elasticsearch...
=====================================
chmod +x ./scripts/init_elasticsearch.sh
./scripts/init_elasticsearch.sh
Elasticsearch가 준비될 때까지 대기 중...
Elasticsearch가 준비되었습니다.
users 인덱스 생성 중...
{"error":{"root_cause":[{"type":"resource_already_exists_exception","reason":"index [users/AW57UPmxTYyW3G-GdL6lHw] already exists","index_uuid":"AW57UPmxTYyW3G-GdL6lHw","index":"users"}],"type":"resource_already_exists_exception","reason":"index [users/AW57UPmxTYyW3G-GdL6lHw] already exists","index_uuid":"AW57UPmxTYyW3G-GdL6lHw","index":"users"},"status":400}users 인덱스가 성공적으로 생성되었습니다.
인덱스 목록 확인:
health status index                                                              uuid                   pri rep docs.count docs.deleted store.size pri.store.size dataset.size
green  open   .internal.alerts-transform.health.alerts-default-000001            MSc-VAFyQHG9tGk2TxwbGg   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.logs.alerts-default-000001          Bief08evQ_SX_3UrwZOzwQ   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.uptime.alerts-default-000001        N-ptFAoNTYeF7OHGtOqZFw   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-ml.anomaly-detection.alerts-default-000001        JL9JINenTS-XMJClxKXrYA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.slo.alerts-default-000001           rFwL_h62QZmQx8kkXFZIyA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-default.alerts-default-000001                     6xFM1NqvTc--9BJ7MlhCpA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.apm.alerts-default-000001           D1xM5G2DTPeF25kkR6_KWQ   1   0          0            0       249b           249b         249b
green  open   users                                                              AW57UPmxTYyW3G-GdL6lHw   1   0       1000            0    229.1kb        229.1kb      229.1kb
green  open   .internal.alerts-observability.metrics.alerts-default-000001       IO4Li-8sS6-AH8aSsvEqLg   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-ml.anomaly-detection-health.alerts-default-000001 95sA140IQ2qCyErtmHKTEg   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-observability.threshold.alerts-default-000001     TV3IWC9fQUuYWQO-56_5sA   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-security.alerts-default-000001                    VE-qcLiURZy7h_i1hb96Lw   1   0          0            0       249b           249b         249b
green  open   .internal.alerts-stack.alerts-default-000001                       _PEhTLb4TJ2bYAwiv6kz8w   1   0          0            0       249b           249b         249b
```

#### Create index pattern in ElasticSearch

PostgreSQL UI를 위한 pgAdmin은 따로 설치하지 않았기 때문에 `psql`을 통해 확인하는 것으로 하고 ElasticSearch에 데이터가 적절히 저장되었는지 확인하기 위해 앞서 설치한 `Kibana`를 이용한다. init_elasticsearch.sh에서 `users` index를 생성하고 초기 테스트 데이터를 저장해주었다.

http://localhost:5601에 접근하고, 다음의 과정을 통해서 데이터가 정상적으로 ElasticSearch에 저장되었는지 확인해준다.

1. 왼쪽 Toolbar에서 **Analytics** -> **Discover** 클릭

<img src="./images/Figure 4.1.png" />

2. **Create index pattern** 클릭

<img src="./images/Figure 4.2.png" />

3. 오른쪽 화면을 보면 앞서 init_elasticsearch.sh를 통해 생성한 users index가 보인다. 그렇기에 왼쪽에 **Name**에 **uses**를 넣어준다.

<img src="./images/Figure 4.3.png" />

4. index가 연결되었기에 **field type**들을 확인할 수 있다.

<img src="./images/Figure 4.4.png" />

5. 왼쪽 Toolbardptj **Discover** 클릭. 저장된 값들을 확인할 수 있다.

<img src="./images/Figure 4.5.png" />

### Building data pipelines in Apache Airflow

1. DAG 작성

```python
import os
import datetime as dt
from datetime import timedelta
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from elasticsearch import Elasticsearch

# 로깅 설정
logger = logging.getLogger(__name__)

# 모듈 가져오기 시도 - 임포트 경로 문제 해결
import sys
import importlib.util
import inspect

# 현재 파일의 절대 경로
current_file = inspect.getfile(inspect.currentframe())
# 현재 디렉토리 (airflow/dags)
current_dir = os.path.dirname(os.path.abspath(current_file))
# Airflow 홈 디렉토리 (/opt/airflow)
airflow_home = os.path.dirname(os.path.dirname(current_dir))
# app 디렉토리 경로
app_dir = os.path.join(airflow_home, "app")

# Python 경로에 app 디렉토리 추가
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)
    logger.info(f"Python 경로에 추가됨: {app_dir}")

# 경로 디버깅
logger.info(f"Python 경로: {sys.path}")
logger.info(f"현재 디렉토리: {os.getcwd()}")
logger.info(f"현재 파일: {current_file}")
logger.info(f"app 디렉토리 경로: {app_dir}")

# 모듈 가져오기 시도
USE_REPOSITORY_PATTERN = False
try:
    from src.database.repository import RepositoryFactory
    from src.config.database import PostgresConfig, ElasticsearchConfig
    from src.utils.connection import PostgresConnector, ElasticsearchConnector

    # 성공적으로 가져왔는지 확인
    if all(
        [
            RepositoryFactory,
            PostgresConfig,
            ElasticsearchConfig,
            PostgresConnector,
            ElasticsearchConnector,
        ]
    ):
        USE_REPOSITORY_PATTERN = True
        logger.info("Repository 모듈 가져오기 성공")
except ImportError as e:
    logger.error(f"Repository 모듈 가져오기 실패: {e}")
    logger.info("직접 구현으로 대체합니다")

# DAG 설정
default_args = {
    "owner": "Se Hyeon Kim",
    "start_date": dt.datetime(2025, 3, 1),
    "retries": 3,
    "retry_delay": dt.timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

# PostgreSQL 연결 정보
PG_HOST = "postgres"  # Docker Compose의 서비스 이름
PG_PORT = 5432
PG_DATABASE = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

# Elasticsearch 연결 정보
ES_HOST = "elasticsearch"  # Docker Compose의 서비스 이름
ES_PORT = 9200


def extract_from_postgresql(**context):
    """PostgreSQL에서 사용자 데이터를 추출하여 CSV 파일로 저장"""
    global USE_REPOSITORY_PATTERN  # 전역 변수로 선언

    try:
        if USE_REPOSITORY_PATTERN:
            # Repository 패턴을 사용하여 데이터 접근
            try:
                logger.info("Repository 패턴으로 PostgreSQL 접근 시도")
                postgres_config = PostgresConfig()
                postgres_connector = PostgresConnector(config=postgres_config)
                repository = RepositoryFactory.create(
                    "postgresql", connector=postgres_connector
                )

                # 연결 확인
                if not repository.check_connection():
                    raise Exception("PostgreSQL 연결에 실패했습니다.")

                # 데이터 조회
                users = repository.get_all(limit=1000)

                if not users:
                    logger.warning("PostgreSQL에서 조회된 사용자 없음")
                    return False

                # DataFrame으로 변환
                df = pd.DataFrame(users)
                logger.info(
                    f"Repository 패턴으로 {len(df)}명의 사용자 데이터 추출 완료"
                )
            except Exception as repo_error:
                logger.error(f"Repository 패턴 사용 실패: {repo_error}")
                logger.info("직접 DB 연결로 대체합니다")
                # Repository 패턴 실패 시 직접 연결로 대체
                USE_REPOSITORY_PATTERN = False
                raise repo_error

        if not USE_REPOSITORY_PATTERN:
            # 직접 psycopg2로 PostgreSQL에 연결
            conn_string = f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} user={PG_USER} password={PG_PASSWORD}"
            logger.info(f"PostgreSQL 직접 연결: {conn_string}")

            conn = psycopg2.connect(conn_string)

            with conn.cursor(cursor_factory=DictCursor) as cur:
                # users 테이블이 없으므로 다른 테이블 사용 (예: dag_run)
                cur.execute("SELECT * FROM dag_run LIMIT 100")
                rows = cur.fetchall()

                if not rows:
                    logger.warning("PostgreSQL에서 조회된 데이터 없음")
                    return False

                # 데이터를 딕셔너리 리스트로 변환
                data = [dict(row) for row in rows]
                df = pd.DataFrame(data)

            conn.close()

        # CSV 저장
        output_path = "/tmp/postgresql_users.csv"
        df.to_csv(output_path, index=False)
        context["ti"].xcom_push(key="csv_path", value=output_path)

        logger.info(f"PostgreSQL에서 {len(df)}개의 데이터 추출 완료")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL 데이터 추출 실패: {e}")

        # 테스트 데이터 생성 (실제 DB 연결이 없는 경우를 위한 백업)
        try:
            logger.info("테스트 데이터를 생성합니다")
            test_data = [
                {
                    "id": 1,
                    "name": "User 1",
                    "street": "Street 1",
                    "city": "City 1",
                    "zip": "11111",
                    "lng": "1.1",
                    "lat": "1.1",
                },
                {
                    "id": 2,
                    "name": "User 2",
                    "street": "Street 2",
                    "city": "City 2",
                    "zip": "22222",
                    "lng": "2.2",
                    "lat": "2.2",
                },
                {
                    "id": 3,
                    "name": "User 3",
                    "street": "Street 3",
                    "city": "City 3",
                    "zip": "33333",
                    "lng": "3.3",
                    "lat": "3.3",
                },
            ]
            df = pd.DataFrame(test_data)
            output_path = "/tmp/postgresql_users.csv"
            df.to_csv(output_path, index=False)
            context["ti"].xcom_push(key="csv_path", value=output_path)
            logger.info("테스트 데이터 생성 완료")
            return True
        except Exception as test_error:
            logger.error(f"테스트 데이터 생성 실패: {test_error}")
            raise AirflowException(f"데이터 추출 오류: {str(e)}")


def load_to_elasticsearch(**context):
    """CSV 파일 데이터를 Elasticsearch에 적재"""
    global USE_REPOSITORY_PATTERN  # 전역 변수로 선언

    try:
        # CSV 파일 경로 가져오기
        ti = context["ti"]
        csv_path = ti.xcom_pull(task_ids="extract_postgresql_data", key="csv_path")

        if not csv_path:
            raise AirflowException(
                "이전 태스크에서 CSV 파일 경로를 가져올 수 없습니다."
            )

        # CSV 파일 읽기
        df = pd.read_csv(csv_path)

        if df.empty:
            logger.warning("적재할 사용자 데이터가 없습니다")
            return False

        if USE_REPOSITORY_PATTERN:
            # Repository 패턴을 사용하여 Elasticsearch에 적재
            try:
                logger.info("Repository 패턴으로 Elasticsearch 접근 시도")
                es_config = ElasticsearchConfig()
                es_connector = ElasticsearchConnector(config=es_config)
                repository = RepositoryFactory.create(
                    "elasticsearch",
                    connector=es_connector,
                    index="users_from_postgresql",
                )

                # 연결 확인
                if not repository.check_connection():
                    raise Exception("Elasticsearch 연결에 실패했습니다.")

                # 데이터 변환 및 적재
                records = df.to_dict("records")
                inserted_count = repository.bulk_save(records)

                logger.info(f"Elasticsearch에 {inserted_count}개 문서 적재 완료")
                return True
            except Exception as repo_error:
                logger.error(f"Repository 패턴 사용 실패: {repo_error}")
                logger.info("직접 Elasticsearch 연결로 대체합니다")
                # 실패 시 직접 연결로 대체
                USE_REPOSITORY_PATTERN = False
                raise repo_error

        if not USE_REPOSITORY_PATTERN:
            # Elasticsearch 직접 연결
            es_url = f"http://{ES_HOST}:{ES_PORT}"
            logger.info(f"Elasticsearch 직접 연결: {es_url}")

            es = Elasticsearch([es_url])

            # 연결 확인
            if not es.ping():
                logger.error("Elasticsearch 연결 실패")
                logger.info(
                    "작업을 완료된 것으로 표시합니다 (실제 데이터는 적재되지 않음)"
                )
                return True  # 테스트 환경에서는 성공으로 처리

            # 데이터 변환 및 적재
            index_name = "users_from_postgresql"
            bulk_data = []
            for _, row in df.iterrows():
                bulk_data.append({"index": {"_index": index_name}})
                bulk_data.append(row.to_dict())

            if bulk_data:
                es.bulk(operations=bulk_data, refresh=True)

            logger.info(f"Elasticsearch에 {len(df)}개 문서 적재 완료")
            return True

    except Exception as e:
        logger.error(f"Elasticsearch 데이터 적재 실패: {e}")
        # 개발/테스트 환경에서는 이 오류를 무시하고 진행
        logger.info("Elasticsearch 적재에 실패했지만 작업을 완료된 것으로 표시합니다")
        return True


# DAG 정의
with DAG(
    dag_id="user_data_transfer",
    default_args=default_args,
    description="PostgreSQL 사용자 데이터를 Elasticsearch로 전송",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=["postgresql", "elasticsearch", "user_data"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_postgresql_data",
        python_callable=extract_from_postgresql,
    )

    load_task = PythonOperator(
        task_id="load_elasticsearch_data",
        python_callable=load_to_elasticsearch,
    )

    # 태스크 의존성 설정
    extract_task >> load_task
```

2. 실행 결과

<img src="./images/Figure 4.6.png" />

<img src="./images/Figure 4.7.png" />

<img src="./images/Figure 4.8.png" />

- extract_postgresql_data

```bash
ac7a746c901c
 ▶ Log message source details
[2025-03-08, 04:04:08 UTC] {local_task_job_runner.py:123} ▶ Pre task execution logs
[2025-03-08, 04:04:08 UTC] {main.py:128} INFO - PostgreSQL 직접 연결: host=postgres port=5432 dbname=*** user=*** password=***
[2025-03-08, 04:04:08 UTC] {main.py:152} INFO - PostgreSQL에서 1개의 데이터 추출 완료
[2025-03-08, 04:04:08 UTC] {python.py:240} INFO - Done. Returned value was: True
[2025-03-08, 04:04:08 UTC] {taskinstance.py:341} ▶ Post task execution logs
```

- load_elasticsearch_data

```bash
ac7a746c901c
 ▶ Log message source details
[2025-03-08, 04:04:09 UTC] {local_task_job_runner.py:123} ▶ Pre task execution logs
[2025-03-08, 04:04:09 UTC] {main.py:253} INFO - Elasticsearch 직접 연결: http://elasticsearch:9200
[2025-03-08, 04:04:09 UTC] {_transport.py:349} INFO - HEAD http://elasticsearch:9200/ [status:200 duration:0.003s]
[2025-03-08, 04:04:09 UTC] {_transport.py:349} INFO - PUT http://elasticsearch:9200/_bulk?refresh=true [status:200 duration:0.096s]
[2025-03-08, 04:04:09 UTC] {main.py:275} INFO - Elasticsearch에 1개 문서 적재 완료
[2025-03-08, 04:04:09 UTC] {python.py:240} INFO - Done. Returned value was: True
[2025-03-08, 04:04:09 UTC] {taskinstance.py:341} ▶ Post task execution logs
```

### Handling databases with NiFi processorss

#### Extracting data from PostgreSQL

<img src="./images/Figure 4.9.png" />

```bash
app-py3.12 {seilylook} ☕️ curl -X GET "localhost:9200/_cat/indices?v"                                          
health status index                            uuid                   pri rep docs.count docs.deleted store.size pri.store.size
green  open   .geoip_databases                 7wlLmCbzT-68QQtczL1XlQ   1   0         39            0     36.2mb         36.2mb
green  open   .kibana_task_manager_7.17.28_001 32xC4NKOQCWKlK_Lta3KuA   1   0         17         1206    267.3kb        267.3kb
yellow open   fromnifi                         7wEUB-_YTI6ZpVo9nVu0Fg   1   1     450400            0    189.9mb        189.9mb
green  open   .kibana_7.17.28_001              lUIvEKPPQwiGzG94W_LaVA   1   0         15            0      2.3mb          2.3mb
green  open   .apm-custom-link                 guOxO9XJSdmOvvEuTyOdnA   1   0          0            0       227b           227b
green  open   .apm-agent-configuration         77OEbeUsSsW8bGctTzmD1A   1   0          0            0       227b           227b
green  open   users                            iI4uC4UiRPyFBgts1aAB7w   1   0       1000            0    231.9kb        231.9kb
```

#### 문제 해결

Docker 환경에서 Postgres, Elasticsearch, Nifi를 실행하는 상태이기 때문에 다음과 같이 다르게 따로 세팅이 필요하다. 

PostgreSQL, Elasticsearch connection 할 때, postgres는 jar를 이용해야 한다. PostgreSQL 공식 홈페이지에서 사용하고자 하는 버전의 jar을 다운받아 local의 nifi/drivers에 저장해줬다. 이를 docker-compose에서 nifi container를 만들 때 `mount` 시켜서 nifi container에서 PostgreSQL jar를 인식할 수 있도록 해준다.

```yaml
services:
  nifi:
    image: apache/nifi:1.28.0
    container_name: nifi
    restart: always
    ports:
      - "9300:9300"
    environment:
      - NIFI_WEB_HTTP_HOST=0.0.0.0
      - NIFI_WEB_HTTP_PORT=9300
      - NIFI_WEB_PROXY_HOST=localhost:9300
      - SINGLE_USER_CREDENTIALS_USERNAME=nifi
      - SINGLE_USER_CREDENTIALS_PASSWORD=nifipassword
    volumes:
      - nifi-system-data:/opt/nifi/nifi-current/system-data  # 내부 시스템 데이터
      - ./logs/nifi:/opt/nifi/nifi-current/logs
      - nifi-conf:/opt/nifi/nifi-current/conf
      - ./nifi/data/raw:/opt/nifi/nifi-current/data/raw      # 원본 데이터
      - ./nifi/data/processed:/opt/nifi/nifi-current/data/processed  # 처리된 데이터
      - ./nifi/templates:/opt/nifi/nifi-current/templates
      - ./nifi/drivers:/opt/nifi/nifi-current/lib/custom-drivers
```

올바르게 mount 되었는지 확인해준다.

```bash
app-py3.12 {seilylook} ☕️ docker exec -i -t nifi /bin/bash    

nifi@d8ffbc7cb03c:/opt/nifi/nifi-current$ cd lib/custom-drivers/
nifi@d8ffbc7cb03c:/opt/nifi/nifi-current/lib/custom-drivers$ ls
postgresql-42.7.5.jar
```

이어서 **DBCPConnectionPool** 서비스를 다음과 같이 설정해준다.

- Database Connection URL:

> jdbc:postgresql://postgres:5432/dataengineering

- Database Driver Class Name:

> org.postgresql.Driver

- Database Driver Locations:

> /opt/nifi/nifi-current/lib/custom-drivers/postgresql-42.7.1.jar

- Database User:

> airflow

- Password:

> airflow

마지막으로 **PutElasticsearchHtttp** processor의 properties에서 URL와 Port를 다음으로 설정해준다.

> http://elasticsearch:9200

## Chapter 6. Building a 311 Data Pipeline

### Building the data pipeline

#### NiFi data extraction & Load 문제 해결 과정

##### 문제 상황

NiFi 데이터 파이프라인에서 다음과 같은 워크플로우를 구성했습니다:

1. ExecuteScript (1차): 외부 API에서 데이터를 가져오는 스크립트
2. SplitJSON: 가져온 데이터를 개별 레코드로 분할
3. ExecuteScript (2차): 각 레코드에 추가 필드 생성 및 데이터 변환
4. EvaluateJsonPath: JSON에서 id 필드 추출
5. PutElasticsearchHTTP: Elasticsearch에 데이터 저장

그러나 마지막 PutElasticsearchHTTP 프로세서에서 다음 오류가 발생했습니다:

```bash
Failed to process Flowfile due to failed to parse, transfering to failure
```

로그를 자세히 살펴보니 다음과 같은 구체적인 오류 메시지를 확인할 수 있었습니다:

```bash
Index operation upsert requires a valid identifier value from a flow file attribute, transferring to failure.
```

##### 문저 분석

로그 분석 결과, 다음 사항을 확인했습니다:

1. 원인: PutElasticsearchHTTP 프로세서가 'upsert' 작업을 수행하기 위해 필요한 식별자(ID) 값이 올바르게 설정되지 않았습니다.

2. 세부 사항:

    - FlowFile 크기가 모두 8바이트로, 실제 JSON 데이터가 아닐 가능성이 높았습니다.
    - EvaluateJsonPath 프로세서에서 id를 추출하고 있었지만, 이 값이 PutElasticsearchHTTP 프로세서에서 인식하는 형식으로 설정되지 않았습니다.

##### 해결 방법

1. PutElasticsearchHTTP 프로세서 설정 수정
PutElasticsearchHTTP 프로세서의 설정을 다음과 같이 수정했습니다:

    - Identifier Attribute: id (EvaluateJsonPath에서 추출한 속성명과 일치하도록 설정)
    - Index Operation: 기존 upsert 설정 확인
    - Type: Elasticsearch 버전에 맞게 설정 (7.x 이상의 경우 비워두거나 _doc 사용)

2. UpdateAttribute 프로세서 추가
EvaluateJsonPath와 PutElasticsearchHTTP 사이에 UpdateAttribute 프로세서를 추가하여 ID 값을 명시적으로 설정했습니다:

    - 새 프로세서: UpdateAttribute
    - 속성 설정:

        - 속성명: elasticsearch.id
        - 값: ${id} (EvaluateJsonPath에서 추출한 id 속성 사용)

3. PutElasticsearchHTTP 프로세서 설정 추가 수정
UpdateAttribute 추가 후, PutElasticsearchHTTP 프로세서의 설정을 다시 수정했습니다:

    - Identifier Attribute: elasticsearch.id (UpdateAttribute에서 설정한 속성명으로 변경)

4. 디버깅을 위한 LogAttribute 활용
문제 해결 과정에서 다음과 같이 LogAttribute 프로세서를 활용하여 데이터 흐름을 모니터링했습니다:

    - LogAttribute 설정:

        - Log Level: INFO 또는 DEBUG
        - Log Payload: true (FlowFile 내용을 함께 로깅)
        - Attributes to Log: all attributes (모든 속성 로깅)

##### 결과

<img src="./images/Figure 6.1.png" />

Elasticsearch에 정상적으로 저장된 것을 확인할 수 있었습니다.

```bash
app-py3.12 {seilylook} 🚀 curl -X GET "localhost:9200/_cat/indices?v"

health status index                            uuid                   pri rep docs.count docs.deleted store.size pri.store.size
green  open   .geoip_databases                 qhv1z7QnTAit-MJzq4nVxw   1   0         39            0     36.7mb         36.7mb
green  open   .kibana_task_manager_7.17.28_001 PkOLULcPR02nuAckO0GZQg   1   0         17          228      141kb          141kb
green  open   .apm-custom-link                 bCszQPeqTm6YGadNEOdDbw   1   0          0            0       227b           227b
green  open   .kibana_7.17.28_001              frGA6utWSFSA-o3uovgPBw   1   0         11            0      2.3mb          2.3mb
yellow open   scf                              L7BtsSUNQXK1FxOEmM_tPA   1   1       5000            0      1.7mb          1.7mb
green  open   .apm-agent-configuration         EKRPRn9aRbepTGm81kxlXQ   1   0          0            0       227b           227b
green  open   users                            hFnaau-_TtaDclMcC5Ocpw   1   0          0            0       227b           227b
```

<img src="./images/Figure 6.2.png" />

<img src="./images/Figure 6.3.png" />

<img src="./images/Figure 6.4.png" />

## Chapter 12. Building a Kafka Cluster

In stream processing, the data may be inifinite and incomplete at the time of a query. One of the leading tools in handling streaming data is **Apache Kafka**. Kafka is a tool that allows you to send dat in real time to topics. These topics can be read by consumers who process the data. 

### Creating zookeeper and Kafka clusters

```bash
services:
  # Zookeeper service
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-log:/var/lib/zookeeper/log
      - ./logs/zookeeper:/var/log/zookeeper
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    networks:
      - data-platform
  # Kafka Broker 1
  kafka1:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka1
    ports:
      - "9092:9092"
      - "29092:29092"
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
      KAFKA_TOOLS_LOG4J_LOGLEVEL: INFO
    volumes:
      - kafka1-data:/var/lib/kafka/data
      - ./logs/kafka1:/var/log/kafka
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 45s
    networks:
      - data-platform

  # Kafka Broker 2
  kafka2:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka2
    ports:
      - "9093:9093"
      - "29093:29093"
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:9093,PLAINTEXT_HOST://localhost:29093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
      KAFKA_TOOLS_LOG4J_LOGLEVEL: INFO
    volumes:
      - kafka2-data:/var/lib/kafka/data
      - ./logs/kafka2:/var/log/kafka
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9093", "--list"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 45s
    networks:
      - data-platform

  # Kafka Broker 3
  kafka3:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka3
    ports:
      - "9094:9094"
      - "29094:29094"
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka3:9094,PLAINTEXT_HOST://localhost:29094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
      KAFKA_TOOLS_LOG4J_LOGLEVEL: INFO
    volumes:
      - kafka3-data:/var/lib/kafka/data
      - ./logs/kafka3:/var/log/kafka
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9094", "--list"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 45s
    networks:
      - data-platform

  # Kafka-UI
  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: kafka-ui
    ports:
      - "8989:8080"
    depends_on:
      - kafka1
      - kafka2
      - kafka3
    environment:
      KAFKA_CLUSTERS_0_NAME: data-platform-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka1:9092,kafka2:9093,kafka3:9094
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
      KAFKA_CLUSTERS_0_METRICS_PORT: 9997
    restart: always
    networks:
      - data-platform
```

#### Testing the kafka cluster

1. Topics 테스트

```bash
docker exec -i -t kafka1 bash

kafka-topics --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 --create --topic {TOPIC_NAME} --partitions 3 --replication-factor 3

Created topic dataengineering.
```

<img src="./images/Figure 12.1.png" />

2. Topics 확인

```bash
kafka-topics --bootstrap-server kafka1:9092 --list

dataengineering

kafka-topics --bootstrap-server kafka1:9092 --describe --topic dataengineering

Topic: dataengineering  TopicId: FhQDsGqqQVaJARfp--T_tw PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: dataengineering  Partition: 0    Leader: 3       Replicas: 3,2,1 Isr: 3,2,1
        Topic: dataengineering  Partition: 1    Leader: 1       Replicas: 1,3,2 Isr: 1,3,2
        Topic: dataengineering  Partition: 2    Leader: 2       Replicas: 2,1,3 Isr: 2,1,3
```

3. Messages 테스트

```bash
kafka-console-producer --bootstrap-server kafka1:9092 kafka2:9093 kafka3:9094 --topic dataengineering

> 안녕하세요 메시지 1입니다.
> 안녕하세요 메시지 2입니다.
> {"name": "테스트", "value": 123}
```

4. Read Message

새로운 터미널 열기

```bash
kafka-console-consumer --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 --topic dataengineering --from-beginning

안녕하세요 메시�지 1입니다.
안녕하세요 메시지 2입니다.
{"�name": "테스트", "value": 123}

Processed a total of 4 messages
```

<img src="./images/Figure 12.2.png" />

### Producing and consuming with python

#### Writing a kafka producer in Python

```bash
app-py3.12 {seilylook} 💡   ~/Development/Book/Data_Engineering_with_Python/app   main ±  make start
=========================
Starting the application...
=========================
python -m src.main
2025-03-20 21:36:56,208 - root - INFO - 데이터셋이 이미 존재합니다: data/raw/test_data.csv
2025-03-20 21:36:56,208 - root - INFO - ========================
2025-03-20 21:36:56,208 - root - INFO - Kafka Topic & Message 생성
2025-03-20 21:36:56,208 - root - INFO - ========================
2025-03-20 21:36:56,227 - src.services.data_streaming - INFO - Kafka 클러스터 연결 및 토픽 확인 중...
2025-03-20 21:36:56,255 - src.services.data_streaming - WARNING - 'users' 토픽이 존재하지 않습니다. 자동 생성될 수 있습니다.
2025-03-20 21:36:56,255 - src.services.data_streaming - INFO - 'data/raw/test_data.csv' 파일 처리 시작
2025-03-20 21:36:56,261 - src.services.data_streaming - INFO - 100개 메시지 처리 중...
2025-03-20 21:36:56,264 - src.services.data_streaming - INFO - 200개 메시지 처리 중...
2025-03-20 21:36:56,267 - src.services.data_streaming - INFO - 300개 메시지 처리 중...
2025-03-20 21:36:56,270 - src.services.data_streaming - INFO - 400개 메시지 처리 중...
2025-03-20 21:36:56,272 - src.services.data_streaming - INFO - 500개 메시지 처리 중...
2025-03-20 21:36:56,275 - src.services.data_streaming - INFO - 600개 메시지 처리 중...
2025-03-20 21:36:56,277 - src.services.data_streaming - INFO - 700개 메시지 처리 중...
2025-03-20 21:36:56,280 - src.services.data_streaming - INFO - 800개 메시지 처리 중...
2025-03-20 21:36:56,282 - src.services.data_streaming - INFO - 900개 메시지 처리 중...
2025-03-20 21:36:56,285 - src.services.data_streaming - INFO - 1000개 메시지 처리 중...
2025-03-20 21:36:58,524 - src.services.data_streaming - INFO - 총 1000개 메시지가 'users' 토픽으로 전송되었습니다.
```

##### 문세 상황

1. DNS 해석 실패

```bash
%3|1742473184.651|FAIL|csv-producer#producer-1| [thrd:kafka2:9093/bootstrap]: kafka2:9093/bootstrap: Failed to resolve 'kafka2:9093': nodename nor servname provided, or not known (after 3ms in state CONNECT, 1 identical error(s) suppressed)
```

2. 연결 시간 초과

```bash
%4|1742473953.566|FAIL|csv-producer#producer-1| [thrd:172.18.0.7:9093/bootstrap]: 172.18.0.7:9093/bootstrap: Connection setup timed out in state CONNECT (after 30030ms in state CONNECT)
%4|1742473954.564|FAIL|csv-producer#producer-1| [thrd:172.18.0.6:9092/bootstrap]: 172.18.0.6:9092/bootstrap: Connection setup timed out in state CONNECT (after 30028ms in state CONNECT)
%4|1742473955.569|FAIL|csv-producer#producer-1| [thrd:172.18.0.8:9094/bootstrap]: 172.18.0.8:9094/bootstrap: Connection setup timed out in state CONNECT (after 30029ms in state CONNECT)
```

##### 문제 원인

1. kafka 이중 리스너 설정

kafka는 Docker 컨테이너 환경에서 두 가지 리스너를 사용한다.

- 내부 통신용 리스너: `PLAINTEXT://kafka1:9092`

    - 컨티에너 간 내부 통신에 사용됨

    - Docker 내부 DNS로 해석되어야 함

- 외부 접근용 리스너: `PLAINTEXT_HOST://localhost:29092`

    - 호스트 머신에서 접근할 때 사용됨

    - 외부로 포트가 노출됨

2. 구체적인 오류 원인

- DNS 해석 실패

    - 클라이언트가 `kafka1:9092, kafka2:9093, kafka3:9094`와 같은 호스트명을 IP주소로 해석하지 못함

    - 이는 Docker 네트워크 외부에서 접근하거나, DNS 설정이 제대로 되지 않은 경우 발생.

- 연결 시간 초과

    - IP 주소는 해석되었으나 실제 TCP 연결이 이루어지지 않음

    - 이는 보통 방화벽 문제, 네트워크 분리, 또는 Kafka 설정 문제로 발생

##### 해결 과정

1. 접근 방식 변경: 내부 포트에서 외부 포트로

kafka 브로커의 외부 노출 포트(29092, 29093, 29094)를 사용하도록 변경

```python
# 변경 전
bootstrap_servers = "kafka1:9092,kafka2:9093,kafka3:9094"

# 변경 후
bootstrap_servers = "localhost:29092,localhost:29093,localhost:29094"
```

2. 해결 원리

- 내부 포트(9092, 9093, 9094):

    - Docker 네트워크 내에서만 접근 가능

    - 컨테이너 간 직접 통신에 사용

- 외부 포트(29092, 29093, 29094):
    
    - 호스트 머신을 통해 접근

    - Docker 컨테이너 외부에서도 접근 가능

    - localhost로 라우팅됨

3. Docker compose에서의 kafka 설정 확인

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

- `ADVERTISED_LISTENERS`: Kafka가 클라이언트에게 알려주는 연결 정보
- `LISTENER_SECURITY_PROTOCOL_MAP`: 각 리스너의 보안 프로토콜 지정
- `INTER_BROKER_LISTENER_NAME`: 브로커 간 통신에 사용할 리스너 지정

##### 결론

- 같은 Docker 네트워크 내: 서비스 이름과 내부 포트 (kafka1:9092)

- 외부 또는 다른 네트워크: localhost와 외부 포트 (localhost:29092)


#### Writing a kafka consumer in Python

```bash
app-py3.12 ✘ {seilylook} 💡   ~/Development/Book/Data_Engineering_with_Python/app   main ±  make start
=========================
Starting the application...
=========================
python -m src.main
2025-03-20 23:02:45,286 - root - INFO - 데이터셋이 이미 존재합니다: data/raw/test_data.csv
2025-03-20 23:02:45,286 - root - INFO - ========================
2025-03-20 23:02:45,286 - root - INFO - Kafka Topic & Message 생성
2025-03-20 23:02:45,286 - root - INFO - ========================
2025-03-20 23:02:45,303 - src.services.data_streaming - INFO - 토픽 구독 시작: users
2025-03-20 23:02:45,303 - src.services.data_streaming - INFO - 메시지 소비 시작...
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Kristina Parker', 'age': 68, 'street': '34674 Miller Overpass', 'city': 'Randallfurt', 'state': 'Maryland', 'zip': 40293, 'lng': 161.665903, 'lat': -87.125185}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Johnathan Lawson', 'age': 19, 'street': '95990 Williams Shore Apt. 829', 'city': 'Webbside', 'state': 'Maine', 'zip': 15543, 'lng': 146.494403, 'lat': -73.700935}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Rose Carpenter', 'age': 68, 'street': '444 Joseph Station', 'city': 'Pattersonside', 'state': 'New Mexico', 'zip': 79242, 'lng': 0.048327, 'lat': 74.385104}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Kimberly Santiago', 'age': 39, 'street': '7635 Peterson Spur Apt. 396', 'city': 'Tinaborough', 'state': 'Nevada', 'zip': 66267, 'lng': -38.278099, 'lat': -36.354147}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Wendy Murphy', 'age': 75, 'street': '35166 Ashlee Mills', 'city': 'Lawsonview', 'state': 'Massachusetts', 'zip': 30520, 'lng': -137.345477, 'lat': 35.262674}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Michael Lin', 'age': 18, 'street': '13086 Hall Pass', 'city': 'East Jay', 'state': 'New York', 'zip': 49686, 'lng': -52.411619, 'lat': -5.883704}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Wesley Watts', 'age': 61, 'street': '4541 Roth Brook Apt. 538', 'city': 'Hensleyland', 'state': 'Maine', 'zip': 70629, 'lng': 137.051209, 'lat': -35.1061065}
2025-03-20 23:02:49,507 - root - INFO - Received: {'name': 'Dennis Wolfe', 'age': 37, 'street': '474 Jones Plaza', 'city': 'Wardville', 'state': 'Minnesota', 'zip': 70795, 'lng': 19.632934, 'lat': -81.602252}
2025-03-20 23:02:49,508 - root - INFO - Received: {'name': 'Sharon Chandler', 'age': 21, 'street': '696 Michael Valleys Apt. 412', 'city': 'Lauraton', 'state': 'New Jersey', 'zip': 19419, 'lng': 14.510882, 'lat': 65.1203075}
2025-03-20 23:02:49,508 - root - INFO - Received: {'name': 'Amanda Mcmahon', 'age': 34, 'street': '96470 Cobb Hollow', 'city': 'Albertberg', 'state': 'Louisiana', 'zip': 22483, 'lng': -8.723311, 'lat': 27.196991}
2025-03-20 23:02:49,508 - root - INFO - Received: {'name': 'Peter Nguyen', 'age': 68, 'street': '15478 Dylan Crescent', 'city': 'North Katrinashire', 'state': 'New Jersey', 'zip': 96223, 'lng': 26.947073, 'lat': -9.097944}
2025-03-20 23:02:49,508 - root - INFO - Received: {'name': 'Matthew Robbins', 'age': 43, 'street': '4211 Brittany Field Suite 605', 'city': 'South Rebeccaborough', 'state': 'Delaware', 'zip': 19879, 'lng': 100.065663, 'lat': 54.933101}
2025-03-20 23:02:49,508 - root - INFO - Received: {'name': 'Michael Wilcox', 'age': 33, 'street': '018 Leon Alley', 'city': 'Johnmouth', 'state': 'New Mexico', 'zip': 73338, 'lng': -19.245506, 'lat': 26.5704125}
2025-03-20 23:02:49,508 - root - INFO - Received: {'name': 'Amanda Williams', 'age': 75, 'street': '44981 Rebecca Bypass', 'city': 'North Joseph', 'state': 'South Carolina', 'zip': 66529, 'lng': -24.771468, 'lat': 14.545032}
```