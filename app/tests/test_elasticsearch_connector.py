import unittest
from unittest.mock import patch, MagicMock
import logging

from src.config.database import ElasticsearchConfig
from src.utils.connection import ElasticsearchConnector


class TestElasticsearchConnector(unittest.TestCase):
    """ElasticsearchConnector 클래스에 대한 테스트"""

    def setUp(self):
        """각 테스트 실행 전 설정"""
        self.config = ElasticsearchConfig()
        self.config.hosts = ["https://es-test:9200"]
        self.config.index = "test-index"
        self.config.username = "elastic"
        self.config.password = "password"
        self.config.use_ssl = True
        self.config.verify_certs = True

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_init_with_config(self, mock_create_client):
        """컨피그를 지정하여 초기화했을 때 올바르게 설정되는지 확인"""
        # 모의 클라이언트 생성
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(self.config)

        self.assertEqual(connector.config, self.config)
        self.assertIsNotNone(connector.client)
        self.assertEqual(connector.client, mock_client)
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_init_without_config(self, mock_create_client):
        """컨피그를 지정하지 않고 초기화했을 때 기본 설정이 사용되는지 확인"""
        # 모의 클라이언트 생성
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector()

        self.assertIsInstance(connector.config, ElasticsearchConfig)
        self.assertListEqual(connector.config.hosts, ["http://localhost:9200"])
        self.assertEqual(connector.config.index, "users")
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_connect_success(self, mock_create_client):
        """Elasticsearch 클라이언트 생성 성공 테스트"""
        # 모의 클라이언트 생성
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(self.config)

        self.assertEqual(connector.client, mock_client)
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_connect_exception(self, mock_create_client):
        """Elasticsearch 클라이언트 생성 실패 테스트"""
        # 예외 발생 시나리오
        mock_create_client.side_effect = Exception("Connection error")

        connector = ElasticsearchConnector(self.config)

        self.assertIsNone(connector.client)
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_get_client(self, mock_create_client):
        """get_client 메서드 테스트"""
        # 모의 클라이언트 생성
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(self.config)
        client = connector.get_client()

        self.assertEqual(client, mock_client)
        mock_create_client.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_check_connection_success(self, mock_create_client):
        """연결 확인 성공 테스트"""
        # 모의 클라이언트 설정
        mock_client = MagicMock()
        mock_client.info.return_value = {"version": {"number": "7.10.0"}}
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(self.config)
        result = connector.check_connection()

        self.assertTrue(result)
        mock_client.info.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_check_connection_exception(self, mock_create_client):
        """연결 확인 실패 테스트"""
        # 모의 클라이언트 설정 - info 메서드에서 예외 발생
        mock_client = MagicMock()
        mock_client.info.side_effect = Exception("API error")
        mock_create_client.return_value = mock_client

        connector = ElasticsearchConnector(self.config)
        result = connector.check_connection()

        self.assertFalse(result)
        mock_client.info.assert_called_once()

    @patch("src.utils.connection.ElasticsearchConnector._create_client")
    def test_check_connection_no_client(self, mock_create_client):
        """클라이언트가 없을 때 연결 확인 테스트"""
        # 클라이언트 생성 실패 시나리오
        mock_create_client.side_effect = Exception("Connection error")

        connector = ElasticsearchConnector(self.config)
        result = connector.check_connection()

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
