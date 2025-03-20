import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_postgres_config():
    """PostgreSQL 설정 모의 객체 픽스처"""
    from src.config.database import PostgresConfig

    config = PostgresConfig()
    config.host = "test-host"
    config.port = 5432
    config.dbname = "test-db"
    config.user = "test-user"
    config.password = "test-password"

    return config


@pytest.fixture
def mock_elasticsearch_config():
    """Elasticsearch 설정 모의 객체 픽스처"""
    from src.config.database import ElasticsearchConfig

    config = ElasticsearchConfig()
    config.hosts = ["https://es-test:9200"]
    config.index = "test-index"
    config.username = "elastic"
    config.password = "password"
    config.use_ssl = True
    config.verify_certs = True

    return config


@pytest.fixture
def mock_psycopg2_connect():
    """psycopg2.connect 모의 객체 픽스처"""
    with patch("psycopg2.connect") as mock_connect:
        # 모의 연결 및 커서 객체 설정
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        yield mock_connect, mock_conn, mock_cur
