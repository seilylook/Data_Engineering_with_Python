import pytest
from unittest.mock import patch, MagicMock
import psycopg2

from src.config.database import PostgresConfig, ElasticsearchConfig
from src.utils.connection import PostgresConnector, ElasticsearchConnector


class TestPostgresConfigPytest:
    """PostgresConfig 클래스에 대한 테스트 (pytest 버전)"""

    def test_default_values(self):
        """기본 설정값이 올바르게 설정되는지 확인"""
        config = PostgresConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.dbname == "dataengineering"
        assert config.user == "airflow"
        assert config.password == "airflow"

    def test_connection_string(self):
        """connection_string 속성이 올바른 형식으로 반환되는지 확인"""
        config = PostgresConfig()
        expected = "host=localhost port=5432 dbname=dataengineering user=airflow password=airflow"

        assert config.connection_string == expected


class TestElasticsearchConfigPytest:
    """ElasticsearchConfig 클래스에 대한 테스트 (pytest 버전)"""

    def test_default_values(self):
        """기본 설정값이 올바르게 설정되는지 확인"""
        config = ElasticsearchConfig()

        assert config.hosts == ["http://localhost:9200"]
        assert config.index == "users"
        assert config.username is None
        assert config.password is None
        assert config.use_ssl is False
        assert config.verify_certs is False

    def test_auth_enabled_variations(self):
        """다양한 인증 정보 시나리오에서 auth_enabled가 올바르게 동작하는지 확인"""
        config = ElasticsearchConfig()

        # 인증 정보 없음
        assert config.auth_enabled is False

        # 사용자 이름만 있을 때
        config.username = "elastic"
        assert config.auth_enabled is False

        # 비밀번호만 있을 때
        config.username = None
        config.password = "elastic"
        assert config.auth_enabled is False

        # 인증 정보가 모두 있을 때
        config.username = "elastic"
        config.password = "elastic"
        assert config.auth_enabled is True


class TestPostgresConnectorPytest:
    """PostgresConnector 클래스에 대한 테스트 (pytest 버전)"""

    def test_init_with_config(self, mock_postgres_config):
        """컨피그를 지정하여 초기화했을 때 올바르게 설정되는지 확인"""
        connector = PostgresConnector(mock_postgres_config)

        assert connector.config == mock_postgres_config
        assert connector._conn is None
        assert connector._cur is None

    def test_connect_success(self, mock_postgres_config, mock_psycopg2_connect):
        """연결 성공 시나리오 테스트"""
        mock_connect, mock_conn, mock_cur = mock_psycopg2_connect
        connector = PostgresConnector(mock_postgres_config)

        # connect 컨텍스트 매니저 테스트
        with connector.connect() as cur:
            assert cur == mock_cur

        # 메서드 호출 확인
        mock_connect.assert_called_once_with(mock_postgres_config.connection_string)
        mock_conn.cursor.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_connect_exception(self, mock_postgres_config):
        """연결 중 예외 발생 테스트"""
        with patch("psycopg2.connect") as mock_connect:
            # 모의 객체 설정 - 예외 발생
            mock_connect.side_effect = psycopg2.Error("Connection error")
            connector = PostgresConnector(mock_postgres_config)

            # connect 호출 시 예외가 발생하는지 확인
            with pytest.raises(psycopg2.Error):
                with connector.connect() as cur:
                    pass

            # 메서드 호출 확인
            mock_connect.assert_called_once_with(mock_postgres_config.connection_string)

    def test_connect_query_exception(self, mock_postgres_config, mock_psycopg2_connect):
        """쿼리 실행 중 예외 발생 테스트"""
        mock_connect, mock_conn, mock_cur = mock_psycopg2_connect

        # 쿼리 실행 중 예외 발생 시나리오
        mock_cur.execute.side_effect = psycopg2.Error("Query error")
        connector = PostgresConnector(mock_postgres_config)

        # connect 호출 및 쿼리 실행 중 예외가 발생하는지 확인
        with pytest.raises(psycopg2.Error):
            with connector.connect() as cur:
                cur.execute("SELECT 1")

        # 메서드 호출 확인
        mock_connect.assert_called_once_with(mock_postgres_config.connection_string)
        mock_conn.cursor.assert_called_once()
        mock_conn.rollback.assert_called_once()  # 롤백 확인
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_check_connection_success(
        self, mock_postgres_config, mock_psycopg2_connect
    ):
        """연결 확인 성공 테스트"""
        mock_connect, mock_conn, mock_cur = mock_psycopg2_connect
        connector = PostgresConnector(mock_postgres_config)

        # 연결 확인 테스트
        result = connector.check_connection()

        # 결과 및 메서드 호출 확인
        assert result is True
        mock_cur.execute.assert_called_once_with("SELECT 1")


class TestElasticsearchConnectorPytest:
    """ElasticsearchConnector 클래스에 대한 테스트 (pytest 버전)"""

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_init_with_config(self, mock_create_client, mock_elasticsearch_config):
        """컨피그를 지정하여 초기화했을 때 올바르게 설정되는지 확인"""
        # 모의 클라이언트 생성
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(mock_elasticsearch_config)

        assert connector.config == mock_elasticsearch_config
        assert connector.client == mock_client
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_connect_exception(self, mock_create_client, mock_elasticsearch_config):
        """Elasticsearch 클라이언트 생성 실패 테스트"""
        # 예외 발생 시나리오
        mock_create_client.side_effect = Exception("Connection error")

        connector = ElasticsearchConnector(mock_elasticsearch_config)

        assert connector.client is None
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_get_client(self, mock_create_client, mock_elasticsearch_config):
        """get_client 메서드 테스트"""
        # 모의 클라이언트 생성
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(mock_elasticsearch_config)
        client = connector.get_client()

        assert client == mock_client
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_check_connection_success(
        self, mock_create_client, mock_elasticsearch_config
    ):
        """연결 확인 성공 테스트"""
        # 모의 클라이언트 설정
        mock_client = MagicMock()
        mock_client.info.return_value = {"version": {"number": "7.10.0"}}
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(mock_elasticsearch_config)
        result = connector.check_connection()

        assert result is True
        mock_client.info.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_check_connection_exception(
        self, mock_create_client, mock_elasticsearch_config
    ):
        """연결 확인 실패 테스트"""
        # 모의 클라이언트 설정 - info 메서드에서 예외 발생
        mock_client = MagicMock()
        mock_client.info.side_effect = Exception("API error")
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(mock_elasticsearch_config)
        result = connector.check_connection()

        assert result is False
        mock_client.info.assert_called_once()
