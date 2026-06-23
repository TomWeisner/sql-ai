import sql_ai
from sql_ai import (
    AthenaBackend,
    AthenaConfig,
    AwsConfig,
    BedrockConfig,
    RedshiftBackend,
    RedshiftConfig,
    SqlLLM,
    Table,
)
from sql_ai.config import AthenaConfig as AthenaConfigModuleExport
from sql_ai.config import AwsConfig as AwsConfigModuleExport
from sql_ai.config import BedrockConfig as BedrockConfigModuleExport
from sql_ai.config import RedshiftConfig as RedshiftConfigModuleExport
from sql_ai.sql_backends.athena.athena_backend import (
    AthenaBackend as AthenaModuleExport,
)
from sql_ai.sql_backends.redshift.redshift_backend import (
    RedshiftBackend as RedshiftModuleExport,
)
from sql_ai.sql_backends.table import Table as TableModuleExport
from sql_ai.sql_llm import SqlLLM as SqlLLMModuleExport


def test_public_api_re_exports_core_symbols():
    assert AwsConfig is AwsConfigModuleExport
    assert BedrockConfig is BedrockConfigModuleExport
    assert AthenaConfig is AthenaConfigModuleExport
    assert RedshiftConfig is RedshiftConfigModuleExport
    assert Table is TableModuleExport
    assert AthenaBackend is AthenaModuleExport
    assert RedshiftBackend is RedshiftModuleExport
    assert SqlLLM is SqlLLMModuleExport


def test_public_api_does_not_export_legacy_config_facade():
    assert not hasattr(sql_ai, "Config")
