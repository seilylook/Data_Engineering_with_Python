import csv
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from faker import Faker
from src.modules.progress_bar import print_progress_bar
import time


@dataclass
class DatasetConfig:
    """Configuration for fake dataset generation."""

    filename: str = "test_data.csv"
    record_count: int = 1_000_000
    output_dirs: List[str] = None
    header: List[str] = None

    def __post_init__(self):
        if self.header is None:
            self.header = [
                "name",
                "age",
                "street",
                "city",
                "state",
                "zip",
                "lng",
                "lat",
            ]

        if self.output_dirs is None:
            self.output_dirs = ["./data/raw", "../nifi/data/raw"]


class DataGenerator:
    """Generator for fake datasets using Faker library."""

    def __init__(self, config: Optional[DatasetConfig] = None):
        """Initialize generator with optional custom configuration."""
        self.config = config or DatasetConfig()
        self.fake = Faker()
        self.logger = logging.getLogger(__name__)

    def create_sample_dataset(self) -> Tuple[List[Path], int]:
        """Generate a sample dataset with random personal information.

        Returns:
            Tuple containing a list of output paths and the number of records generated
        """
        self._ensure_output_directories()

        # Create primary file first
        primary_output_path = Path(self.config.output_dirs[0]) / self.config.filename
        record_count = self._write_records(primary_output_path)

        # Copy to additional locations
        output_paths = [primary_output_path]
        for output_dir in self.config.output_dirs[1:]:
            target_path = Path(output_dir) / self.config.filename
            shutil.copy2(primary_output_path, target_path)
            output_paths.append(target_path)
            self.logger.info(f"✓ Copied data to {target_path}")

        self.logger.info(
            f"✓ Successfully generated {record_count:,} records in {len(output_paths)} locations"
        )
        return output_paths, record_count

    def _ensure_output_directories(self) -> None:
        """Create all output directories if they don't exist."""
        for directory in self.config.output_dirs:
            Path(directory).mkdir(parents=True, exist_ok=True)

    def _write_records(self, output_path: Path) -> int:
        """Write fake records to CSV file and return count of records written.

        Args:
            output_path: Path where the CSV file will be written

        Returns:
            int: Number of records written
        """
        with open(output_path, mode="w", newline="") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(self.config.header)

            # Initialize the progress bar
            print_progress_bar(
                iteration=0,
                total=self.config.record_count,
                prefix="Progress",
                suffix="Complete",
                length=50,
            )

            # Generate and write records one by one
            for i in range(self.config.record_count):
                writer.writerow(self._generate_record())
                time.sleep(0.1)  # Simulate processing time

                # Update the progress bar
                print_progress_bar(
                    iteration=i + 1,
                    total=self.config.record_count,
                    prefix="Progress",
                    suffix="Complete",
                    length=50,
                )

        return self.config.record_count

    def _generate_record(self) -> List:
        """Generate a single fake data record."""
        return [
            self.fake.name(),
            self.fake.random_int(min=18, max=80),
            self.fake.street_address(),
            self.fake.city(),
            self.fake.state(),
            self.fake.zipcode(),
            self.fake.longitude(),
            self.fake.latitude(),
        ]
