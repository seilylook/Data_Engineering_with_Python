import unittest
from unittest.mock import patch, MagicMock, call
import psycopg2
from psycopg2.extensions import cursor
import logging

from src.config.database import PostgresConfig
from src.utils.connection import PostgresConnector


class TestPostgresConnector(unittest.TestCase):
    """PostgresConnector 클래스에 대한 테스트"""

    def setUp(self):
        """각 테스트 실행 전 설정"""
        self.config = PostgresConfig()
        self.config.host = "test-host"
        self.config.port = 5432
        self.config.dbname = "test-db"
        self.config.user = "test-user"
        self.config.password = "test-password"

        self.connector = PostgresConnector(self.config)

    @patch("psycopg2.connect")
    def test_init_with_config(self, mock_connect):
        """컨피그를 지정하여 초기화했을 때 올바르게 설정되는지 확인"""
        connector = PostgresConnector(self.config)

        self.assertEqual(connector.config, self.config)
        self.assertIsNone(connector._conn)
        self.assertIsNone(connector._cur)
        # psycopg2.connect가 호출되지 않았는지 확인
        mock_connect.assert_not_called()

    @patch("psycopg2.connect")
    def test_init_without_config(self, mock_connect):
        """컨피그를 지정하지 않고 초기화했을 때 기본 설정이 사용되는지 확인"""
        connector = PostgresConnector()

        self.assertIsInstance(connector.config, PostgresConfig)
        self.assertEqual(connector.config.host, "localhost")
        # psycopg2.connect가 호출되지 않았는지 확인
        mock_connect.assert_not_called()

    @patch("psycopg2.connect")
    def test_connect_success(self, mock_connect):
        """연결 성공 시나리오 테스트"""
        # 모의 객체 설정
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        # connect 컨텍스트 매니저 테스트
        with self.connector.connect() as cur:
            self.assertEqual(cur, mock_cur)

        # 메서드 호출 확인
        mock_connect.assert_called_once_with(self.config.connection_string)
        mock_conn.cursor.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("psycopg2.connect")
    def test_connect_exception(self, mock_connect):
        """연결 중 예외 발생 테스트"""
        # 모의 객체 설정 - 예외 발생
        mock_conn = MagicMock()
        mock_connect.side_effect = psycopg2.Error("Connection error")

        # connect 호출 시 예외가 발생하는지 확인
        with self.assertRaises(psycopg2.Error):
            with self.connector.connect() as cur:
                pass

        # 메서드 호출 확인
        mock_connect.assert_called_once_with(self.config.connection_string)
        mock_conn.rollback.assert_not_called()  # 연결 자체가 실패했으므로 rollback 호출 없음

    @patch("psycopg2.connect")
    def test_connect_query_exception(self, mock_connect):
        """쿼리 실행 중 예외 발생 테스트"""
        # 모의 객체 설정
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        # 쿼리 실행 중 예외 발생 시나리오
        mock_cur.execute.side_effect = psycopg2.Error("Query error")

        # connect 호출 및 쿼리 실행 중 예외가 발생하는지 확인
        with self.assertRaises(psycopg2.Error):
            with self.connector.connect() as cur:
                cur.execute("SELECT 1")

        # 메서드 호출 확인
        mock_connect.assert_called_once_with(self.config.connection_string)
        mock_conn.cursor.assert_called_once()
        mock_conn.rollback.assert_called_once()  # 롤백 확인
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("psycopg2.connect")
    def test_check_connection_success(self, mock_connect):
        """연결 확인 성공 테스트"""
        # 모의 객체 설정
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        # 연결 확인 테스트
        result = self.connector.check_connection()

        # 결과 및 메서드 호출 확인
        self.assertTrue(result)
        mock_connect.assert_called_once_with(self.config.connection_string)
        mock_cur.execute.assert_called_once_with("SELECT 1")

    @patch("psycopg2.connect")
    def test_check_connection_failure(self, mock_connect):
        """연결 확인 실패 테스트"""
        # 모의 객체 설정 - 예외 발생
        mock_connect.side_effect = psycopg2.Error("Connection error")

        # 연결 확인 테스트
        result = self.connector.check_connection()

        # 결과 및 메서드 호출 확인
        self.assertFalse(result)
        mock_connect.assert_called_once_with(self.config.connection_string)


if __name__ == "__main__":
    unittest.main()
