import logging
import luigi
from pathlib import Path
from .data_transforms import download_random_users_to_file


logger = logging.getLogger("luigi")


class DownloadRandomUsers(luigi.Task):

    workdir = luigi.PathParameter(default=".")
    n_record = luigi.IntParameter(default=3)  # , description="number of times to hit the API")

    def output(self):
        return luigi.LocalTarget(Path(self.workdir) / "raw" / "randomusers.txt")

    def run(self):
        with self.output().temporary_path() as temp_output_path:
            download_random_users_to_file(logger, temp_output_path, n_record=self.n_record)
