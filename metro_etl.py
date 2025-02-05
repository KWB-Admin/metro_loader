import polars, os, logging
from kwb_loader import loader
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
        logger.info("Successfully transformed data")
    except:
        logging.exception("")
    data.write_parquet(transformed_parquet_path)
    logger.info("Successfully wrote .parquet file")


def clean_up_numbers(data: polars.DataFrame, numcol: str) -> polars.DataFrame:
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


if __name__ == "__main__":
    logger.info(
        "--------------- Metro ETL ran on %s ----------------" % (datetime.today())
    )

    if not os.listdir("data_dump"):
        logger.info("No data is available for loading, quitting program.")
        exit()

    data_type = ""
    for data_file in os.listdir("data_dump"):
        if "Depth" in data_file:
            etl_yaml = load(open("yaml/monitoring_etl_variables.yaml", "r"), Loader)
        elif "Production" in data_file:
            etl_yaml = load(open("yaml/recovery_etl_variables.yaml", "r"), Loader)

        remove_DOM_data(
            old_csv_path=etl_yaml["old_csv"], new_csv_path=etl_yaml["new_csv"]
        )

        polars_schema = etl_yaml["polars_schema"]
        polars_schema = {col: polars.String for col in polars_schema}

        transform_data(
            new_csv_path=etl_yaml["new_csv"],
            schema=polars_schema,
            cols_to_drop=etl_yaml["cols_to_drop"],
            new_col_names_dict=etl_yaml["new_col_names"],
            transformed_parquet_path=etl_yaml["transformed_parquet"],
        )

        loader.load(
            credentials=(user, host, password),
            dbname=etl_yaml["db_name"],
            schema=etl_yaml["schema"],
            table_name=etl_yaml["table_name"],
            data_path=etl_yaml["transformed_parquet"],
            prim_key=etl_yaml["prim_key"],
        )
        logging.info(
            "Successfully loaded data into %s.%s.%s \n"
            % (etl_yaml["db_name"], etl_yaml["schema"], etl_yaml["table_name"])
        )
        os.remove(etl_yaml["new_csv"])
        os.remove(etl_yaml["old_csv"])
        os.rename(
            etl_yaml["transformed_parquet"],
            "loaded_data/metro_data_loaded_%s.parquet"
            % (datetime.date(datetime.today())),
        )
