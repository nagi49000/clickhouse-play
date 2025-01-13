import logging
import luigi
from datetime import datetime, UTC
from pathlib import Path
from .data_transforms import (
    download_random_users_to_file,
    schemaed_csv_rows_to_file,
)

logger = logging.getLogger("luigi")


class DownloadRandomUsers(luigi.Task):

    workdir = luigi.PathParameter(default=".")
    work_subdir = luigi.PathParameter(default=datetime.now(UTC).isoformat(timespec="seconds"))
    n_record = luigi.IntParameter(default=3)

    def output(self):
        return luigi.LocalTarget(Path(self.workdir) / "raw" / self.work_subdir / "randomusers.csv")

    def run(self):
        with self.output().temporary_path() as temp_output_path:
            download_random_users_to_file(logger, temp_output_path, n_record=self.n_record)


class SchemaedCsvRows(luigi.Task):
    workdir = luigi.PathParameter(default=".")

    def requires(self):
        return DownloadRandomUsers(workdir=self.workdir)

    def output(self):
        return {
            "valid": luigi.LocalTarget(
                Path(self.workdir) / "schemaed"
            ),
            "invalid": luigi.LocalTarget(
                Path(self.workdir) / "schemaed-failed"
            ),
        }

    def run(self):
        with self.output()["valid"].temporary_path() as valid_path:
            with self.output()["invalid"].temporary_path() as invalid_path:
                schemaed_csv_rows_to_file(logger, Path(self.workdir) / "raw", Path(valid_path), Path(invalid_path))
