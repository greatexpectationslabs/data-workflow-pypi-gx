# Databricks notebook source
# import custom utility modules
from utils import repo_utils, gx_utils
from utils.notebook_utils import Notebook
import spark_utils

# other required imports
import pandas as pd
from pandas_gbq import read_gbq
from pyspark.sql import functions as F

# COMMAND ----------

# create a Notebook
# contains attributes for notebook, filestystem, git, etc.
nb = Notebook()

# show attributes
nb.attributes

# COMMAND ----------

# create a RepoConfig
# contains attributes for notebook, parameters, config, etc.
rc = repo_utils.get_repo_config(config_file=nb.config_file)

# show attributes
rc.attributes

# COMMAND ----------

# get a date range for the data based on notebook params
# dates must be in ISO-8601 format!
# also specify the pypi package name of interest

date_range = [
    dbutils.widgets.get("param_dt_begin"),
    dbutils.widgets.get("param_dt_end"),
]

pypi_pkg = dbutils.widgets.get("param_pypi_pkg")


print(
    f"Querying PyPI downloads of {pypi_pkg} from '{date_range[0]}' to '{date_range[1]}'."
)

# COMMAND ----------

# query to get data from public database
# must use BigQuery SQL syntax!
query = f"""
select
  timestamp as download_time,
  country_code,
  file.type as file_type,
  file.version as pkg_version,
  coalesce(details.python, details.implementation.version) as python_version,
  details.installer.name as installer_name,
  details.installer.version as installer_version,
  details.distro.name as distro_name,
  details.distro.version as distro_version,
  details.system.name as system_name,
  details.system.release as system_version,
  date(timestamp) as dt
from
  `bigquery-public-data.pypi.file_downloads`
where
  file.project = "{pypi_pkg}"
  and date(timestamp) between "{date_range[0]}"
  and "{date_range[1]}"
"""

# COMMAND ----------

# use pandas_gbq library to get a pandas dataframe of query results
df = read_gbq(query, use_bqstorage_api=True)

# COMMAND ----------

# inspect pandas dataframe schema
df.info()

# COMMAND ----------

# get a gx validator using the expectation suite in RepoConfig
# if "overwrite=True" the suite will be overwritten and given default validations
validator = gx_utils.default_validator(
    pandas_df=df, date_range=date_range, overwrite=False
)

# COMMAND ----------

# create a checkpoint for validating pandas dataframe against expectation suite
checkpoint = gx_utils.default_checkpoint(
    pandas_df=df,
    validator=validator,
    evaluation_parameters={
        ep: validator.get_evaluation_parameter(ep) for ep in ["min_ts", "max_ts"]
    },
)

# COMMAND ----------

# run the checkpoint against the validator
checkpoint_run = checkpoint.run()

# COMMAND ----------

# create a results object
results = gx_utils.CheckpointRunResult(checkpoint_run)

# COMMAND ----------

# raise an error if expecations failed > expectations failures allowed
results.check_results(failures_allowed=0)

# COMMAND ----------

# list failures from validation results
results.list_failures()

# COMMAND ----------

# column order
select_cols = [
    "download_time",
    "country_code",
    "file_type",
    "gx",
    "py",
    "installer",
    "distro",
    "system",
    "dt",
]

# transform pandas dataframe w/ spark
df_spark = (
    spark.createDataFrame(df)
    .withColumnRenamed("pkg_version", "gx")
    .withColumnRenamed("python_version", "py")
    .withColumn(
        "installer",
        F.struct(
            F.col("installer_name").alias("name"),
            F.col("installer_version").alias("version"),
        ),
    )
    .withColumn(
        "distro",
        F.struct(
            F.col("distro_name").alias("name"),
            F.col("distro_version").alias("version"),
        ),
    )
    .withColumn(
        "system",
        F.struct(
            F.col("system_name").alias("name"),
            F.col("system_version").alias("version"),
        ),
    )
    .select(*[F.col(col) for col in select_cols])
)

# COMMAND ----------

# define the output table schema and name
target = spark_utils.HiveTable(
    name=f"oss_events.pypi_downloads",
    schema_definition="""
    {0} table {1} 
    (
    download_time timestamp comment "timestamp of download", 
    country_code string comment "2-digit ISO country code", 
    file_type string comment "type of file downloaded",
    gx string comment "gx release version",
    py string comment "python release version",
    installer struct<name: string, version: string> comment "installer name and version", 
    distro struct<name: string, version: string> comment "distro name and version",
    system struct<name: string, version: string> comment "system name and version"
    )
    partitioned by (dt date comment "partition date %Y-%m-%d")
    comment "downloads of gx oss from python package index"
    """,
)

target.attributes

# COMMAND ----------

# write to target table in hive metastore
target.insert_into(df_spark)
