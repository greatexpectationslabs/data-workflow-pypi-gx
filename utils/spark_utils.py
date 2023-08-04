from datetime import datetime
from dataclasses import dataclass
import pandas as pd
from pandas import Timestamp as pandasTimestamp
from itertools import chain
from collections import namedtuple
from typing import Any, Optional, Union
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


# get spark session
spark = SparkSession.builder.getOrCreate()


@dataclass
class HiveTable:
    """
    Dataclass for a Hive metastore table
    """

    name: str
    schema_definition: Optional[str] = None

    def __post_init__(self):
        self.database: str = self.get_table_namespace().database
        self.table: str = self.get_table_namespace().table
        self.exists: bool = self.check_if_exists()
        self.row_count: Union[int, None] = self.get_table_counts().row
        self.dt_count: Union[int, None] = self.get_table_counts().dt
        self.max_dt: Union[str, None] = self.get_max_dt()
        self.last_updated: datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.attributes: dict[str] = {
            k: v for k, v in self.__dict__.items() if not k in ["schema_definition"]
        }
        self.check_schema_definition()

    def get_table_namespace(self) -> tuple[str]:
        """
        Make sure table name is valid.
        Returns: tuple with (database, table)
        """
        split_text = self.name.split(".")
        if not len(split_text) == 2:
            raise ValueError("name should be like 'database.table'.")
        
        db_name = split_text[0]
        db_list = [
            db.databaseName for db in list(spark.sql("show databases").collect())
        ]
        if not db_name in db_list:
            raise ValueError(
                f"database '{db_name}' does not exist and must be created."
            )
        
        TableNamespace = namedtuple("TableNamespace", "database table")
        
        return TableNamespace(split_text[0], split_text[1])

    def check_schema_definition(self) -> None:
        """
        Make sure schema definition has valid first line.
        """
        if not self.schema_definition:
            pass
        else:  
            sw = "{0} table {1}"
            if not self.schema_definition.strip().startswith(sw):
                raise ValueError(f"Schema definition statement must start with '{sw}'.")

    def check_if_exists(self) -> bool:
        """
        Return True if table already exists in metastore
        """
        exists = True
        try:
            spark.sql(f"describe {self.name}")
        except:
            exists = False

        return exists
    
    def get_table_counts(self) -> tuple[Union[int, None]]:
        """
        Get row count and dt (partition) count
        """
        TableCount = namedtuple("TableCount", "row dt")
        
        if not self.exists:
            return TableCount(None, None)
        else:
            row_cnt = spark.sql(f"select count(*) from {self.name}")
            dt_cnt = spark.sql(f"select count(distinct dt) from {self.name}")
            return TableCount(row_cnt.collect()[0][0], dt_cnt.collect()[0][0])
        
    def create_table_from_schema(self, replace_if_exists: bool = False) -> None:
        """
        Create or replace table using schema.
        """

        if self.exists and not replace_if_exists:
            raise ValueError("Table exists but 'replace_if_exists' arg is False.")

        elif self.exists and replace_if_exists:
            print(f"Existing table {self.name} replaced.")
            action = "replace"

        else:
            print(f"New table {self.name} created.")
            action = "create"

        spark.sql(self.schema_definition.format(action, self.name))
        self.exists = True
        self.last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_max_dt(self, as_string: bool = True) -> Union[str, datetime, None]:
        """
        Get latest dt value of table
        Args:
            - as_string: True to return a %Y-%m-%d string otherwise datetime
        """
        if not self.exists or self.row_count == 0:
            return None
        
        else:
            sql = f"""select max(dt) as maxdt from {self.name}"""

            maxdt = spark.sql(sql).collect()[0][0]

            maxdt_dt = (
                datetime.fromisoformat(maxdt) if isinstance(maxdt, str) else maxdt
            )

            return maxdt_dt.strftime("%Y-%m-%d") if as_string else maxdt_dt

    def get_schema_summary(self) -> pd.core.frame.DataFrame:
        """
        Get schema details in a pandas dataframe
        """
        return spark.sql(f"describe formatted {self.table}").toPandas()

    def insert_into(
        self,
        spark_df: DataFrame,
        overwrite: bool = False,
    ) -> None:
        """
        Insert contents of an in-memory spark dataframe
        to a hive table using Pyspark `insertInto` method.
        Args:
            - spark_df: in-memory spark dataframe
            - overwrite: True to overwrite existing data in table, False to append
        Returns: None
        """
        spark_df.write.insertInto(self.name, overwrite=overwrite)

    def zorder(self, columns: list[str], quiet: bool = True) -> None:
        """
        Zorder a hive table by specfied columns
        Args:
            - hive_table: database.table to zorder
            - columns: list of column names on which to zorder
            - quiet: False to display zorder results
        Returns: None
        """

        sql = f"""optimize {self.name} zorder by ({", ".join(columns)})"""
        z_results = spark.sql(sql)

        metrics = z_results.select("metrics.*").schema.fieldNames()
        metric_cols = [f"metrics.{m}" for m in metrics]

        metrics_df = z_results.select(
            F.lit(self.name).alias("table"),
            *[F.col(mc) for mc in metric_cols],
        )

        return display(metrics_df.toPandas()) if not quiet else None

    
    def as_dataframe(
        self,
        columns: Optional[list[str]] = None,
        date_range: Optional[list[Union[str, pandasTimestamp]]] = None,
        date_column: Optional[str] = None,
    ) -> DataFrame:
        """
        Query table and return as a DataFrame
        """
        df = spark.read.table(self.name)
        columns = [col for col in df.columns] if not columns else columns
        if not date_range:
            return select_cols(df, cols_to_select=columns)
        else:
            date_column = "dt" if not date_column else date_column
            columns = columns + [date_column]
            df = select_cols(df, cols_to_select=columns)
            
            # handle if date range has strings/timestamps
            col_type = [col[1] for col in df.dtypes if col[0] == date_column][0]
            range_is_ts = isinstance(date_range[0], pandasTimestamp)

            # convert range timestamps to strings if needed
            if col_type in ["date", "string"] and range_is_ts:
                date_range = [f"{dt.date().strftime('%Y-%m-%d')}" for dt in date_range]

            return df.filter(
                F.col(date_column).between(
                    F.lit(min(date_range)), F.lit(max(date_range))
                )
            )

            # convert range strings to timestamps if needed
            if col_type == "timestamp" and not range_is_ts:
                date_range = date_utils.get_dt_range(
                    date_range[0], date_range[-1], with_timestamps=True
                )

                return df.filter(
                    F.col(date_column).between(
                        F.lit(min(date_range)), F.lit(max(date_range))
                    )
                )

            else:
                raise ValueError("Mismatch between date column type and date_range type.")


def temp_view(spark_df: DataFrame, view_name: Optional[str] = None) -> None:
    """
    Register a dataframe as a SQL temp view
    Args:
        - spark_df: an in-memory DataFrame
    Returns:
        - None
    """
    view_name = "tmp" if not view_name else view_name

    return spark_df.createOrReplaceTempView(view_name)


def get_pandas_sample(
    spark_df: DataFrame,
    sample_pct: Optional[float] = None,
) -> pd.core.frame.DataFrame:
    """
    Convert a spark dataframe to downsampled (or not...) pandas dataframe
    Args:
        - spark_df: spark dataframe
        - sample_pct: % sample of original to return; default is 0.1 (10%)
    Returns: pandas dataframe with sample_pct % of the spark dataframe
    """

    sample_pct = 0.1 if not sample_pct else sample_pct

    if not 0 <= sample_pct <= 1:
        raise ValueError("sample_pct must be a float between 0 and 1")

    return spark_df.sample(fraction=sample_pct).toPandas()


def list_to_sql_format(pylist: list[Any]) -> str:
    """
    Convert a python list to a sql-friendly string.
    """
    return str([str(e) for e in pylist]).replace("[", "(").replace("]", ")")


def list_hive_databases() -> list[str]:
    """
    Get a list of all existing Hive databases
    """
    return [db.databaseName for db in list(spark.sql("show databases").collect())]


def list_hive_tables(database_name: str) -> list[str]:
    """
    Utility function to list tables in a given Hive database
    Args:
        - database_name: name of target database
    Returns: list of table names
    """

    databases = list_hive_databases()
    assert_msg = f"Database '{database_name}' does not exist in Hive metastore."
    assert database_name in databases, assert_msg

    sql = """show tables from {0}""".format(database_name)
    rows = spark.sql(sql).collect()

    return [r["tableName"] for r in rows]


def select_cols(
    spark_df: DataFrame, cols_to_select: list[str], quiet: bool = True
) -> DataFrame:
    """
    Select columns in order from a Spark DataFrame.
    Args:
        - spark_df: a Spark DataFrame
        - cols_to_select: list of column names in order of selection
        - quiet: False to print a list of columns that exist but were not selected
    Returns: Spark DataFrame with selected columns in specified order
    """
    assert len(cols_to_select) > 0, "cols_to_select must have at least one column name."
    cols_in_df = [col for col in spark_df.columns]

    cols_in_both = list(set(cols_in_df).intersection(set(cols_to_select)))
    cols_to_ignore = list(set(cols_in_df) - set(cols_to_select))

    for c in cols_to_select:
        assert c in cols_in_df, f"Column '{c}' is not in the dataframe."

    if not quiet:
        for c in cols_to_ignore:
            print(f"Column '{c}' was not selected.")

    return spark_df.select(list(set(cols_to_select)))


def array_describe(
    spark_df: DataFrame, array_col: str, gb_cols: list[str]
) -> DataFrame:
    """
    Spark helper function for creating a map of descriptive
    stats from an array of numerics.
    Args:
        - spark_df: a spark dataframe
        - array_col: array column in df to describe
        - gb_cols: a list of column names for groupby aggregation
    Returns:
        - a spark dataframe with summary metrics for the groupby columns
    """

    metrics = ["cnt", "cntd", "sum", "min", "max", "median"]

    return (
        spark_df.select(
            *[F.col(c) for c in gb_cols], F.col(array_col).alias("array_col")
        )
        .select(
            *[F.col(c) for c in gb_cols],
            F.col("array_col"),
            F.size(F.col("array_col")).alias("cnt"),
            F.size(F.array_distinct(F.col("array_col"))).alias("cntd"),
            F.array_min(F.col("array_col")).alias("min"),
            F.array_max(F.col("array_col")).alias("max"),
            F.expr("""aggregate(array_col, 0L, (acc,x) -> acc+x)""").alias("sum"),
            F.when(F.size(F.col("array_col")) == 0, 0)
            .otherwise(
                F.element_at(
                    F.col("array_col"), F.floor(F.size(F.col("array_col"))).cast("int")
                )
            )
            .alias("median"),
        )
        .fillna(0)
        .select(
            *[F.col(c) for c in gb_cols],
            F.create_map(list(chain(*((F.lit(m), F.col(m)) for m in metrics)))).alias(
                str.replace(array_col, "_array", "_summary")
            ),
        )
    )
    