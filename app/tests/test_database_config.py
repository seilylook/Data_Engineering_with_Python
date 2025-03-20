import unittest
from src.config.database import PostgresConfig, ElasticsearchConfig


class TestPostgresConfig(unittest.TestCase):
    """PostgresConfig 클래스에 대한 테스트"""

    def test_default_values(self):
        """기본 설정값이 올바르게 설정되는지 확인"""
        config = PostgresConfig()

        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.dbname, "dataengineering")
        self.assertEqual(config.user, "airflow")
        self.assertEqual(config.password, "airflow")

    def test_connection_string(self):
        """connection_string 속성이 올바른 형식으로 반환되는지 확인"""
        config = PostgresConfig()
        expected = "host=localhost port=5432 dbname=dataengineering user=airflow password=airflow"

        self.assertEqual(config.connection_string, expected)

    def test_custom_values(self):
        """사용자 지정 값으로 설정이 가능한지 확인"""
        config = PostgresConfig()
        config.host = "test-host"
        config.port = 1234
        config.dbname = "test-db"
        config.user = "test-user"
        config.password = "test-pass"

        self.assertEqual(config.host, "test-host")
        self.assertEqual(config.port, 1234)
        self.assertEqual(config.dbname, "test-db")
        self.assertEqual(config.user, "test-user")
        self.assertEqual(config.password, "test-pass")

        expected = (
            "host=test-host port=1234 dbname=test-db user=test-user password=test-pass"
        )
        self.assertEqual(config.connection_string, expected)


class TestElasticsearchConfig(unittest.TestCase):
    """ElasticsearchConfig 클래스에 대한 테스트"""

    def test_default_values(self):
        """기본 설정값이 올바르게 설정되는지 확인"""
        config = ElasticsearchConfig()

        self.assertListEqual(config.hosts, ["http://localhost:9200"])
        self.assertEqual(config.index, "users")
        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertFalse(config.use_ssl)
        self.assertFalse(config.verify_certs)

    def test_auth_enabled_with_no_credentials(self):
        """인증 정보가 없을 때 auth_enabled가 False를 반환하는지 확인"""
        config = ElasticsearchConfig()
        self.assertFalse(config.auth_enabled)

    def test_auth_enabled_with_username_only(self):
        """사용자 이름만 있을 때 auth_enabled가 False를 반환하는지 확인"""
        config = ElasticsearchConfig()
        config.username = "elastic"
        self.assertFalse(config.auth_enabled)

    def test_auth_enabled_with_password_only(self):
        """비밀번호만 있을 때 auth_enabled가 False를 반환하는지 확인"""
        config = ElasticsearchConfig()
        config.password = "elastic"
        self.assertFalse(config.auth_enabled)

    def test_auth_enabled_with_credentials(self):
        """인증 정보가 모두 있을 때 auth_enabled가 True를 반환하는지 확인"""
        config = ElasticsearchConfig()
        config.username = "elastic"
        config.password = "elastic"
        self.assertTrue(config.auth_enabled)

    def test_custom_values(self):
        """사용자 지정 값으로 설정이 가능한지 확인"""
        config = ElasticsearchConfig()
        config.hosts = ["https://es-server:9243"]
        config.index = "logs"
        config.username = "admin"
        config.password = "secret"
        config.use_ssl = True
        config.verify_certs = True

        self.assertListEqual(config.hosts, ["https://es-server:9243"])
        self.assertEqual(config.index, "logs")
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertTrue(config.use_ssl)
        self.assertTrue(config.verify_certs)
        self.assertTrue(config.auth_enabled)


if __name__ == "__main__":
    unittest.main()
