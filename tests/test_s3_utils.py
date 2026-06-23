from unittest.mock import MagicMock

from sql_ai.utils.s3_utils import upload_file_to_s3


def test_upload_file_to_s3_without_subfolder_uses_object_key_only():
    s3 = MagicMock()

    upload_file_to_s3(
        s3=s3,
        bucket_name="demo-bucket",
        subfolders=None,
        file_path="/tmp/example.txt",
        object_key="example.txt",
    )

    s3.upload_file.assert_called_once_with(
        "/tmp/example.txt",
        "demo-bucket",
        "example.txt",
    )


def test_upload_file_to_s3_trims_trailing_slash_from_subfolder():
    s3 = MagicMock()

    upload_file_to_s3(
        s3=s3,
        bucket_name="demo-bucket",
        subfolders="reports/",
        file_path="/tmp/example.txt",
        object_key="example.txt",
    )

    s3.upload_file.assert_called_once_with(
        "/tmp/example.txt",
        "demo-bucket",
        "reports/example.txt",
    )
