import os
import logging
import yaml
import pandas as pd
import great_expectations as gx
from scripts.postgres_helper import upload_overwrite_table


PROJECT_DIR = os.getcwd()
DAGS_SCRIPTS_DIR = os.path.abspath(os.path.dirname(__file__))
RAW_DATA_DIR = os.path.join(DAGS_SCRIPTS_DIR, "data_examples")
SCHEMAS_PATH = os.path.join(DAGS_SCRIPTS_DIR, "data_schemas.yaml")
DATA_QUALITY_PATH = os.path.join(DAGS_SCRIPTS_DIR, "data_quality_checks.yaml")


logger = logging.getLogger("CLEVER MAIN")


def validate_data(**kwargs):
    file_name = kwargs.get("file_name")
    dataset = file_name.split(".")[0]
    file_path = os.path.join(RAW_DATA_DIR, file_name)

    logger.info(f"Data Validataion for dataset: {dataset}")

    df = pd.read_csv(
        file_path,
        sep=",",
        quotechar='"',
        escapechar="\\",
    )

    context = gx.get_context()
    asset_name = f"{dataset}_ds"
    data_asset = context.sources.pandas_default.add_dataframe_asset(asset_name, df)
    batch_request = data_asset.build_batch_request()

    validator = context.get_validator(batch_request=batch_request)

    with open(DATA_QUALITY_PATH) as file:
        columns_dict = yaml.load(file, Loader=yaml.SafeLoader)

    columns = columns_dict[dataset]
    for column in columns:
        results = validator.expect_column_values_to_not_be_null(column=column)
        print(results)
        if results.success:
            logger.info("Validation passed!")
        else:
            logger.warning("Validation failed")


def upload_to_postgres(**kwargs):
    file_name = kwargs.get("file_name")
    table_name = file_name.split(".")[0]

    with open(SCHEMAS_PATH) as file:
        schemas_dict = yaml.load(file, Loader=yaml.SafeLoader)

    schema = schemas_dict[table_name]
    logger.info(f"Schema for {table_name}: {schema}")

    columns = schema["columns"]
    logger.info(f"Columns to be uploaded tp Postgres: {columns}")

    column_not_null = schema.get("column_not_null")
    columns_fill_na = schema.get("columns_fill_na")

    raw_df = pd.read_csv(
        os.path.join(RAW_DATA_DIR, file_name),
        sep=",",
        quotechar='"',
        escapechar="\\",
        header=0,
        usecols=columns,
    )

    if column_not_null:
        raw_df = raw_df[~raw_df[column_not_null].isnull()]
    else:
        logger.info("No rows to exclude null values")

    if columns_fill_na:
        raw_df = raw_df.fillna(columns_fill_na)
    else:
        logger.info("No columns to fill null values")

    upload_overwrite_table(raw_df, table_name)
    logger.info(f"Uploaded {raw_df.shape[0]} to Postgres")
