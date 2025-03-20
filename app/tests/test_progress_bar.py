import unittest
from unittest.mock import patch, call
import io
import sys

from src.modules.progress_bar import print_progress_bar


class TestProgressBar(unittest.TestCase):
    """Progress Bar 모듈에 대한 테스트"""

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_initial_progress(self, mock_stdout):
        """초기 진행 상태(0%)가 올바르게 출력되는지 테스트"""
        print_progress_bar(0, 100, prefix="Test", suffix="Complete", length=20)
        expected_output = "\rTest |--------------------| 0.0% Complete"
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_half_progress(self, mock_stdout):
        """중간 진행 상태(50%)가 올바르게 출력되는지 테스트"""
        print_progress_bar(50, 100, prefix="Test", suffix="Complete", length=20)
        expected_output = "\rTest |██████████----------| 50.0% Complete"
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_complete_progress(self, mock_stdout):
        """완료 상태(100%)가 올바르게 출력되는지 테스트"""
        print_progress_bar(100, 100, prefix="Test", suffix="Complete", length=20)
        expected_output = "\rTest |████████████████████| 100.0% Complete\n"
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_zero_total(self, mock_stdout):
        """총 반복 수가 0일 때 처리되는지 테스트"""
        print_progress_bar(0, 0, prefix="Test", suffix="Complete", length=20)
        # 출력이 없어야 함
        self.assertEqual(mock_stdout.getvalue(), "")

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_custom_fill_character(self, mock_stdout):
        """사용자 지정 채우기 문자가 올바르게 사용되는지 테스트"""
        print_progress_bar(5, 10, prefix="Test", suffix="Complete", length=10, fill="#")
        expected_output = "\rTest |#####-----| 50.0% Complete"
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_custom_decimals(self, mock_stdout):
        """소수점 자릿수 지정이 올바르게 적용되는지 테스트"""
        print_progress_bar(
            1, 3, prefix="Test", suffix="Complete", length=10, decimals=2
        )
        expected_output = "\rTest |███-------| 33.33% Complete"
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_progress_sequence(self, mock_stdout):
        """진행 상태가 순차적으로 업데이트되는지 테스트"""

        # 출력 버퍼 초기화를 위한 함수
        def reset_buffer():
            mock_stdout.seek(0)
            mock_stdout.truncate(0)

        total = 5
        expected_outputs = [
            "\rTest |-----| 0.0% Complete",  # 길이 수정 (5로 통일)
            "\rTest |█----| 20.0% Complete",
            "\rTest |██---| 40.0% Complete",
            "\rTest |███--| 60.0% Complete",
            "\rTest |████-| 80.0% Complete",
            "\rTest |█████| 100.0% Complete\n",  # 마지막에만 줄바꿈 유지
        ]

        for i in range(total + 1):
            reset_buffer()
            print_progress_bar(i, total, prefix="Test", suffix="Complete", length=5)
            self.assertEqual(mock_stdout.getvalue(), expected_outputs[i])


if __name__ == "__main__":
    unittest.main()
