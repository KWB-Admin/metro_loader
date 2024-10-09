import polars, os
from kwb_loader import loader

old_file = "data_dump/Well Water Depth.csv"
new_file = "data_dump/cleaned_metro_data.csv"

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
    with open(old_file, "r") as file_input:
        with open(new_file, "w") as output:
            for line in file_input:
                if "Delivery" in line:
                    break
                output.write(line)


def transform_data(remove_unneeded_data):
    """
    This take the new csv file and overwrites it with the correct
    columns
    """
    remove_unneeded_data()
    polars.read_csv(new_file).drop(["Pool1", "Project1"]).rename(
        new_col_names
    ).drop_nulls(subset="measurement").write_csv(new_file)


if __name__ == "__main__":

    transform_data(remove_unneeded_data)

    loader.load(db_name, schema, table_name, new_file, prim_key)

    os.remove(new_file)
