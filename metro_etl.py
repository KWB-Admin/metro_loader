import polars, os, logging
import psycopg2 as pg
from psycopg2 import sql
from numpy import ndarray
from datetime import datetime
from yaml import load, Loader

logging.basicConfig(
    filename="log/metro_etl.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

user = os.getenv("kwb_dw_user")
host = os.getenv("kwb_dw_host")
password = os.getenv("kwb_dw_password")


def remove_DOM_data(old_csv_path: str, new_csv_path: str):
    """
    This take the metro csv file and writes a new file without
    the excess data spit out by metro pertaining to Date-of-Month
    (DOM) data

    Args:
        old_csv_path: str, string path to old data file in
            csv format
        new_csv_path: str, string path to new file which will not
            include DOM data
    """
    with open(old_csv_path, "r") as file_input:
        with open(new_csv_path, "w") as output:
            for line in file_input:
                if "DOM" in line:
                    break
                output.write(line)
    logger.info("Successfully removed DOM Data")


def transform_data(
    new_csv_path: str,
    schema: dict,
    cols_to_drop: list,
    new_col_names_dict: dict,
    transformed_parquet_path: str,
):
    """
    This take the new csv file and creates a parquet with the correct
    columns

    Args:
        new_csv_path: str, path to cleaned csv without delivery data
        schema: dict, schema used for reading csv's with polars. Default
            behavior is for all columns to be read as polars.String
        cols_to_drop: list, list of columns to dropped
        new_col_name_dict: dict, dictionary of column names for renaming
            columns
        transformed_parquet_path: str, path to parquet file for writing
            transformed, cleaned data
    """
    try:
        data = (
            polars.read_csv(new_csv_path, schema=schema)
            .drop(cols_to_drop)
            .rename(new_col_names_dict)
            .with_columns(polars.lit(datetime.today()).alias("date_added"))
        )
        if "monitoring" in new_csv_path:
            data = data.drop_nulls(subset="measurement")
        else:
            data = (
                data.drop_nulls(subset="meter_reading_cfs_unrounded")
                .filter(polars.col("meter_reading_cfs_unrounded") != "0.0")
                .with_columns(polars.col("reading_date").str.to_date(format="%m/%d/%y"))
            )
        logger.info("Successfully transformed data.")
    except:
        logging.exception("")
    data.write_parquet(transformed_parquet_path)
    return data


def clean_up_numbers(data: polars.DataFrame, numcol: str) -> polars.DataFrame:
    """
    This removes special characters such as hypens and commas from
    numerical data so that it can be properly loaded.

    Args:
        data: polars.DataFrame, data to be cleaned
        numcol: column with numerical data
    Returns:
        polars.DataFrame, cleaned data
    """
    data_without_special_chars = data.filter(
        ~(polars.col(numcol).str.contains("-") | polars.col(numcol).str.contains(","))
    ).with_columns(polars.col(numcol).cast(polars.Float64))

    special_chars_data = data.filter(
        (polars.col(numcol).str.contains("-") | polars.col(numcol).str.contains(","))
    ).with_columns(polars.col(numcol).str.replace(",", ""))

    special_chars_cleaned = special_chars_data.with_columns(
        polars.when(polars.col(numcol).str.contains("-"))
        .then(polars.col(numcol).str.replace("-", "").cast(polars.Float64) * -1)
        .otherwise(polars.col(numcol).cast(polars.Float64))
    )
    return polars.concat([data_without_special_chars, special_chars_cleaned])


def get_pg_connection(db_name: str) -> pg.extensions.connection:
    """
    This tests a connection with a postgres database to ensure that
    we're loading into a database that actually exists.

    Args:
        db_name: str, name of database to connect to.
    Returns:
        con: pg.extensions.connection, psycopg connection to pg database
    """
    try:
        con = pg.connect(
            "dbname=%s user=%s host=%s password=%s" % (db_name, user, host, password)
        )
        con.autocommit = True
        logging.info("Successfully connected to %s db" % (db_name))
        return con

    except pg.OperationalError as Error:
        logging.error(Error)


def check_table_exists(con: pg.extensions.connection, schema_name: str, table: str):
    """
    This tests a to ensure the table we'll be writing to exists in
    the postgres schema provided.

    Args:
        con: pg.extensions.connection, psycopg connection to pg
            database
        schema_name: str, name of postgres schema
        table_name: str, name of table
    """
    cur = con.cursor()
    command = sql.SQL(
        """
        Select * from {schema_name}.{table} limit 1  
        """
    ).format(
        schema_name=sql.Identifier(schema_name),
        table=sql.Identifier(table),
    )
    try:
        cur.execute(command)
        if isinstance(cur.fetchall(), list):
            logging.info("Table exists, continue with loading.")
    except pg.OperationalError as Error:
        logging.error(Error)


def load_data_into_pg_warehouse(
    data: polars.DataFrame, etl_yaml: dict, data_params: dict
):
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: polars.DataFrame, data to be loaded into warehouse
        etl_yaml: dict, general variables for the etl process
        data_params: dict, variables for specific data_types, such as
            recovery and monitoring data
    """
    con = get_pg_connection(etl_yaml["db_name"])
    check_table_exists(con, etl_yaml["schema_name"], data_params["table_name"])
    try:
        cur = con.cursor()
        for row in data.to_numpy():
            query = build_load_query(row, etl_yaml, data_params)
            cur.execute(query)
        cur.close()
        con.close()
        logging.info(
            "Data was successfully loaded to %s.%s.%s"
            % (etl_yaml["db_name"], etl_yaml["schema_name"], data_params["table_name"])
        )
    except pg.OperationalError as Error:
        con.close()
        logging.error(Error)


def build_load_query(
    data: ndarray, etl_yaml: dict, data_params: dict
) -> pg.sql.Composed:
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: numpy.ndarray, row of data to be loaded
        etl_yaml: dict, general variables for the etl process
        data_params: dict, variables for specific data_types, such as
            recovery and monitoring data
    Returns:
        pg.sql.Composed, Upsert query used to load data
    """
    col_names = sql.SQL(", ").join(
        sql.Identifier(col) for col in data_params["db_schema"].keys()
    )
    values = sql.SQL(" , ").join(sql.Literal(val) for val in data)
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table} ({col_names}) VALUES ({values})
        ON CONFLICT ({prim_key}) DO UPDATE SET {update_col} = Excluded.{update_col}
        """
    ).format(
        schema_name=sql.Identifier(etl_yaml["schema_name"]),
        table=sql.Identifier(data_params["table_name"]),
        col_names=col_names,
        values=values,
        prim_key=sql.SQL(data_params["prim_key"]),
        update_col=sql.Identifier(data_params["update_col"]),
    )


if __name__ == "__main__":
    logger.info(
        "--------------- Metro ETL ran on %s ----------------" % (datetime.today())
    )

    if not os.listdir("data_dump"):
        logger.info("No data is available for loading, quitting program.\n")
        exit()

    data_type = ""
    for data_file in os.listdir("data_dump"):
        etl_yaml = load(open("yaml/etl_variables.yaml", "r"), Loader)
        if "Depth" in data_file:
            data_type = "monitoring"
        elif "Production" in data_file:
            data_type = "recovery"
        data_params = etl_yaml[data_type]

        remove_DOM_data(
            old_csv_path=data_params["old_csv"], new_csv_path=data_params["new_csv"]
        )

        polars_schema = {col: polars.String for col in data_params["polars_schema"]}

        proc_data = transform_data(
            new_csv_path=data_params["new_csv"],
            schema=polars_schema,
            cols_to_drop=data_params["cols_to_drop"],
            new_col_names_dict=data_params["new_col_names"],
            transformed_parquet_path=data_params["transformed_parquet"],
        )

        load_data_into_pg_warehouse(
            data=proc_data,
            etl_yaml=etl_yaml,
            data_params=data_params,
        )
        os.remove(etl_yaml["new_csv"])
        os.remove(etl_yaml["old_csv"])
        os.rename(
            etl_yaml["transformed_parquet"],
            "loaded_data/metro_%s_data_loaded_%s.parquet"
            % (data_type, datetime.date(datetime.today())),
        )
    logger.info("Succesfully ran Metro ETL.\n")
