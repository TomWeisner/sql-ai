import os
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Optional

import boto3
import yaml
from botocore.exceptions import ClientError
from botocore.session import Session


def read_from_yaml(file_path) -> dict:
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def get_all_files_in_directory(
    directory_path: str,
    suffix: Optional[str] = None,
    modified_within_minutes: Optional[int] = 60,
) -> list[tuple[str, str]]:
    """
    Searches the specified directory and returns a list of all files found at that level,
    optionally filtering by suffix and modification time.

    Args:
        directory_path: The path to the directory to walk
        suffix: An optional file extension suffix to filter by
        modified_within_minutes: If set, only return files modified
        within the last X minutes

    Returns:
        A list of all files found, as tuples of (full path, filename)
    """
    files = []
    now = time.time()
    cutoff_time = (
        now - (modified_within_minutes * 60)
        if modified_within_minutes is not None
        else None
    )

    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            if suffix and not filename.endswith(suffix):
                continue

            full_path = os.path.join(root, filename)

            if cutoff_time and os.path.getmtime(full_path) < cutoff_time:
                continue

            files.append((full_path, filename))

    # Sort alphabetically by filename
    files.sort(key=lambda x: x[1])
    return files


# Search AWS profiles for matching account
def find_aws_profile_by_account_id(target_account_id: str = "382901073838"):
    for profile in Session().available_profiles:
        session = boto3.Session(profile_name=profile)
        try:
            sts = session.client("sts")
            identity = sts.get_caller_identity()
            if identity["Account"] == target_account_id:
                return profile
        except ClientError:
            continue  # Skip invalid/misconfigured profiles

    raise ValueError(f"No AWS profile found for account ID {target_account_id}")


@dataclass
class Timer:
    start: Optional[float] = field(default=None, init=False)
    end: Optional[float] = None

    def __post_init__(self):
        self.start = time.time()

    def stop_timer(self):
        self.end = time.time()

    @property
    def elapsed(self) -> Optional[float]:
        if self.end is None:
            self.stop_timer()
        if self.start and self.end:
            return self.end - self.start
        return None


def time_it(fn):
    """
    Decorator to time the execution of a function.

    This decorator wraps a function and records the time taken to execute it.
    It returns the result of the function along with a Timing object containing
    the start time, end time, and elapsed time of the function execution.

    :param fn: The function to be timed.
    :return: A tuple containing the result of the function execution and a Timing object.
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)
        if not isinstance(result, tuple):
            result = (result,)
        return *result, Timer()

    return wrapper
