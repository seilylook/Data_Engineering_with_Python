import unittest
from src.utils.http_client import fetch_page


class TestHttpRequest(unittest.TestCase):
    def test_fetch_page(self):
        params = {
            "place_url": "bernalillo-county",
            "per_page": 100,
            "status": "Archived",
            "page": 1,
        }
        self.assertIsNotNone(
            fetch_page(
                base_url="https://seeclickfix.com/api/v2/issues",
                params=params,
            )
        )


if __name__ == "__main__":
    unittest.main()
