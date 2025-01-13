import requests
from logging import Logger
from typing import Iterator
from pathlib import Path


def download_random_users(
    logger: Logger, n_record: int, include_header: bool = False
) -> Iterator[str]:
    url = "https://randomuser.me/api/"
    response = requests.get(url, params={"format": "csv", "results": n_record})
    if response.ok:
        response_list = response.text.split("\n")
        if len(response_list) != n_record + 1:
            logger.error(
                f"GET to {url} has retrieved {len(response_list)} rows; expected {n_record + 1}"
            )
        if response.text.startswith("gender,name.title") and not include_header:
            response_list = response_list[1:]  # drop the 1st row, which is header
        for row in response_list:
            yield row + "\n"
    else:
        logger.error(
            f"GET to {url} failed with {response.status_code}: {response.reason}"
        )


def download_random_users_to_file(logger: Logger, output_path: Path, n_record: int = 3):
    with open(output_path, "wt") as f:
        for line in download_random_users(logger, n_record):
            f.write(line)
