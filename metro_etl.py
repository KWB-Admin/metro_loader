import polars, os, logging
from kwb_loader import loader
from datetime import datetime

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

old_csv = "data_dump/Well Water Depth.csv"
new_csv = "data_dump/metro_data_without_delivery.csv"
transformed_parquet = "data_dump/metro_data_ready_to_load.parquet"

db_name = "kwb"
schema = "kcwa"
table_name = "metro_water_depths_post_2020"
prim_key = "state_well_number, reading_date"

new_col_names = {
    "FacilityName1": "state_well_number",
    "Textbox56": "reading_date",
    "Textbox57": "measurement",
}


def remove_unneeded_data():
    """
    This take the metro csv file and writes a new file without
    the excess data spit out by metro pertaining to Delivery
    """
    with open(old_csv, "r") as file_input:
        with open(new_csv, "w") as output:
            for line in file_input:
                if "Delivery" in line:
                    break
                output.write(line)
    logger.info("Successfully removed Delivery Data")


def transform_data():
    """
    This take the new csv file and creates a parquet with the correct
    columns
    """
    try:
        data = (
            polars.read_csv(new_csv)
            .drop(["Pool1", "Project1"])
            .rename(new_col_names)
            .drop_nulls(subset="measurement")
            .with_columns(polars.lit(datetime.today()).alias("date_added"))
        )
        logger.info("Successfully transformed data")
    except:
        logging.exception("")
    data.write_parquet(transformed_parquet)
    logger.info("Successfully wrote .parquet file")


if __name__ == "__main__":
    logger.info(
        "--------------- Metro ETL ran on %s ----------------" % (datetime.today())
    )

    remove_unneeded_data()

    transform_data()

    loader.load(db_name, schema, table_name, transformed_parquet, prim_key)
    logging.info(
        "Successfully loaded data into %s.%s.%s \n" % (db_name, schema, table_name)
    )
    os.remove(new_csv)
    os.remove(old_csv)
    os.rename(
        transformed_parquet.strip("data_dump/"),
        "metro_data_loaded_%s" % (datetime.date(datetime.today())),
    )
