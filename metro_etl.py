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


def remove_unneeded_data(old_csv_path: str, new_csv_path: str):
    """
    This take the metro csv file and writes a new file without
    the excess data spit out by metro pertaining to Delivery

    Args:
        old_csv_path: str, string path to old data file in
            csv format
        new_csv_path: str, string path to new file which will not
            include delivery data
    """
    with open(old_csv_path, "r") as file_input:
        with open(new_csv_path, "w") as output:
            for line in file_input:
                if "Delivery" in line:
                    break
                output.write(line)
    logger.info("Successfully removed Delivery Data")


def transform_data(
    new_csv_path: str, new_col_names_dict: dict, transformed_parquet_path: str
):
    """
    This take the new csv file and creates a parquet with the correct
    columns

    Args:
        new_csv_path: str, path to cleaned csv without delivery data
        new_col_name_dict: dict, dictionary of column names for renaming
            columns
        transformed_parquet_path: str, path to parquet file for writing
            transformed, cleaned data
    """
    try:
        data = (
            polars.read_csv(new_csv_path)
            .drop(["Pool1", "Project1"])
            .rename(new_col_names_dict)
            .drop_nulls(subset="measurement")
            .with_columns(polars.lit(datetime.today()).alias("date_added"))
        )
        logger.info("Successfully transformed data")
    except:
        logging.exception("")
    data.write_parquet(transformed_parquet_path)
    logger.info("Successfully wrote .parquet file")


if __name__ == "__main__":
    logger.info(
        "--------------- Metro ETL ran on %s ----------------" % (datetime.today())
    )

    etl_yaml = load(open("etl_variables.yaml", "r"), Loader)

    remove_unneeded_data(
        old_csv_path=etl_yaml["old_csv"], new_csv_path=etl_yaml["new_csv"]
    )

    transform_data(
        new_csv_path=etl_yaml["new_csv"],
        new_col_names_dict=etl_yaml["new_col_names"],
        transformed_parquet_path=etl_yaml["transformed_parquet"],
    )

    loader.load(
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
        etl_yaml["transformed_parquet"].strip("data_dump/"),
        "metro_data_loaded_%s" % (datetime.date(datetime.today())),
    )
