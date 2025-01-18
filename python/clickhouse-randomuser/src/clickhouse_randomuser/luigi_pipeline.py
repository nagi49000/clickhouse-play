import logging
import luigi
from datetime import datetime, UTC
from pathlib import Path
from .data_transforms import (
    download_random_users_to_file,
    schemaed_csv_rows_to_file,
    valid_rows_to_clickhouse,
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
    work_subdir = luigi.PathParameter(default=datetime.now(UTC).isoformat(timespec="seconds"))

    def requires(self):
        return DownloadRandomUsers(workdir=self.workdir)

    def output(self):
        return luigi.LocalTarget(Path(self.workdir) / "schemaed-processing" / f"{self.work_subdir}.log")

    def run(self):
        raw_dir = Path(str(self.input())).parents[1]  # move up a dir, since there will be a timestamp subdir
        valid_path = Path(self.workdir) / "schemaed"
        invalid_path = Path(self.workdir) / "schemaed-failed"
        with self.output().temporary_path() as temp_output_path:
            schemaed_csv_rows_to_file(
                logger,
                Path(str(temp_output_path)),
                raw_dir,
                Path(valid_path),
                Path(invalid_path)
            )


class ToClickhouse(luigi.Task):
    workdir = luigi.PathParameter(default=".")
    work_subdir = luigi.PathParameter(default=datetime.now(UTC).isoformat(timespec="seconds"))

    def requires(self):
        return SchemaedCsvRows(workdir=self.workdir)

    def output(self):
        return luigi.LocalTarget(Path(self.workdir) / "clickhouse-processing" / f"{self.work_subdir}.log")

    def run(self):
        valid_path = Path(self.workdir) / "schemaed"
        with self.output().temporary_path() as temp_output_path:
            valid_rows_to_clickhouse(
                logger,
                Path(str(temp_output_path)),
                Path(valid_path),
                "clickhouse-server",
                "db_random_user",
                "user",
                clickhouse_username="default",
            )
