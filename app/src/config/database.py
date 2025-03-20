from dataclasses import dataclass


@dataclass
class PostgresConfig:
    def __init__(self):
        self.host = "localhost"
        self.port = 5432
        self.dbname = "dataengineering"
        self.user = "airflow"
        self.password = "airflow"

    @property
    def connection_string(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"


@dataclass
class ElasticsearchConfig:
    """Elasticsearch 설정"""

    def __init__(self):
        self.hosts = ["http://localhost:9200"]
        self.index = "users"
        self.username = None
        self.password = None
        self.use_ssl = False
        self.verify_certs = False

    @property
    def auth_enabled(self) -> bool:
        """인증 사용 여부"""
        return self.username is not None and self.password is not None
