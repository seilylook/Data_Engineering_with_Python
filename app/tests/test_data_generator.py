import pytest
import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.services.data_generation import DatasetConfig, DataGenerator


class TestDatasetConfig:
    """DatasetConfig 클래스에 대한 테스트."""

    def test_default_initialization(self):
        """기본 값으로 DatasetConfig가 올바르게 초기화되는지 테스트."""
        config = DatasetConfig()

        assert config.filename == "test_data.csv"
        assert config.record_count == 1_000_000
        assert config.header == [
            "name",
            "age",
            "street",
            "city",
            "state",
            "zip",
            "lng",
            "lat",
        ]
        assert config.output_dirs == ["./data/raw", "../nifi/data/raw"]

    def test_custom_initialization(self):
        """사용자 정의 값으로 DatasetConfig가 올바르게 초기화되는지 테스트."""
        custom_header = ["id", "name", "email"]
        custom_dirs = ["./custom/path"]

        config = DatasetConfig(
            filename="custom.csv",
            record_count=100,
            output_dirs=custom_dirs,
            header=custom_header,
        )

        assert config.filename == "custom.csv"
        assert config.record_count == 100
        assert config.header == custom_header
        assert config.output_dirs == custom_dirs


class TestDataGenerator:
    """DataGenerator 클래스에 대한 테스트."""

    @pytest.fixture
    def temp_dir(self):
        """테스트에 사용할 임시 디렉토리 생성."""
        with tempfile.TemporaryDirectory() as tmpdirname:
            yield tmpdirname

    @pytest.fixture
    def small_config(self, temp_dir):
        """작은 데이터셋 설정으로 테스트용 설정 생성."""
        return DatasetConfig(
            filename="test_small.csv", record_count=10, output_dirs=[temp_dir]
        )

    def test_initialization(self):
        """기본 설정으로 DataGenerator가 올바르게 초기화되는지 테스트."""
        generator = DataGenerator()

        assert isinstance(generator.config, DatasetConfig)
        assert generator.fake is not None
        assert generator.logger is not None

    def test_initialization_with_config(self, small_config):
        """사용자 정의 설정으로 DataGenerator가 올바르게 초기화되는지 테스트."""
        generator = DataGenerator(small_config)

        assert generator.config == small_config

    def test_ensure_output_directories(self, small_config, temp_dir):
        """출력 디렉토리가 올바르게 생성되는지 테스트."""
        # 중첩된 디렉토리 경로 추가
        nested_dir = os.path.join(temp_dir, "nested/path")
        small_config.output_dirs.append(nested_dir)

        generator = DataGenerator(small_config)
        generator._ensure_output_directories()

        # 모든 출력 디렉토리가 생성되었는지 확인
        for dir_path in small_config.output_dirs:
            assert os.path.isdir(dir_path)

    def test_generate_record(self):
        """단일 레코드가 올바른 형식으로 생성되는지 테스트."""
        generator = DataGenerator()
        record = generator._generate_record()

        # 레코드는 설정의 헤더와 같은 개수의 필드를 가져야 함
        assert len(record) == len(generator.config.header)

        # 타입 확인
        assert isinstance(record[0], str)  # name
        assert isinstance(record[1], int)  # age
        assert 18 <= record[1] <= 80  # age 범위
        assert isinstance(record[2], str)  # street
        assert isinstance(record[3], str)  # city
        assert isinstance(record[4], str)  # state
        assert isinstance(record[5], str)  # zip
        assert isinstance(float(record[6]), float)  # lng
        assert isinstance(float(record[7]), float)  # lat

    @patch("time.sleep")  # sleep 함수를 모의 객체로 대체하여 테스트 시간 단축
    def test_write_records(self, mock_sleep, small_config, temp_dir):
        """레코드가 올바르게 CSV 파일에 작성되는지 테스트."""
        generator = DataGenerator(small_config)
        output_path = Path(temp_dir) / small_config.filename

        # 진행 표시줄 출력을 억제하기 위한 패치
        with patch("src.modules.progress_bar.print_progress_bar"):
            record_count = generator._write_records(output_path)

        # 반환된 레코드 수가 설정과 일치하는지 확인
        assert record_count == small_config.record_count

        # 파일이 생성되었는지 확인
        assert output_path.exists()

        # CSV 파일의 내용 확인
        with open(output_path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            rows = list(reader)

            # 헤더와 데이터의 행 수 확인
            assert len(rows) == small_config.record_count + 1  # 헤더 + 데이터 행

            # 헤더 확인
            assert rows[0] == small_config.header

            # 각 데이터 행의 필드 수 확인
            for row in rows[1:]:
                assert len(row) == len(small_config.header)

    @patch("shutil.copy2")
    @patch("src.services.data_generation.DataGenerator._write_records")
    def test_create_sample_dataset(self, mock_write_records, mock_copy2, small_config):
        """샘플 데이터셋 생성 및 복사 과정 테스트."""
        # 여러 출력 디렉토리 설정
        small_config.output_dirs = ["./dir1", "./dir2", "./dir3"]

        # _write_records 모의 함수가 10을 반환하도록 설정
        mock_write_records.return_value = small_config.record_count

        generator = DataGenerator(small_config)

        # _ensure_output_directories 메서드를 모의 객체로 대체
        with patch.object(generator, "_ensure_output_directories"):
            paths, count = generator.create_sample_dataset()

        # _write_records가 한 번만 호출되었는지 확인
        mock_write_records.assert_called_once()

        # 복사가 출력 디렉토리 수에 맞게 호출되었는지 확인 (첫 번째 디렉토리에는 생성, 나머지는 복사)
        assert mock_copy2.call_count == len(small_config.output_dirs) - 1

        # 반환된 경로 수가 출력 디렉토리 수와 일치하는지 확인
        assert len(paths) == len(small_config.output_dirs)

        # 반환된 레코드 수가 설정과 일치하는지 확인
        assert count == small_config.record_count

    @patch("src.services.data_generation.DataGenerator._write_records")
    @patch("src.services.data_generation.DataGenerator._ensure_output_directories")
    def test_create_sample_dataset_integration(
        self, mock_ensure_dirs, mock_write_records, small_config, temp_dir
    ):
        """샘플 데이터셋 생성의 통합 테스트."""
        # 실제 파일 쓰기를 모의 함수로 대체
        mock_write_records.return_value = small_config.record_count

        # 두 개의 출력 디렉토리 설정
        dir1 = os.path.join(temp_dir, "dir1")
        dir2 = os.path.join(temp_dir, "dir2")
        small_config.output_dirs = [dir1, dir2]

        # 디렉토리 생성
        os.makedirs(dir1, exist_ok=True)
        os.makedirs(dir2, exist_ok=True)

        # 첫 번째 출력 파일 생성 (실제로는 _write_records가 이 작업 수행)
        primary_output_path = Path(dir1) / small_config.filename
        with open(primary_output_path, "w") as f:
            f.write("test content")

        generator = DataGenerator(small_config)
        paths, count = generator.create_sample_dataset()

        # 파일이 각 디렉토리에 존재하는지 확인
        for dir_path in small_config.output_dirs:
            file_path = Path(dir_path) / small_config.filename
            assert file_path.exists()

        # 반환된 경로 및 레코드 수 확인
        assert len(paths) == len(small_config.output_dirs)
        assert count == small_config.record_count


if __name__ == "__main__":
    pytest.main()
