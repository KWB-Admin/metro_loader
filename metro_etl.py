import polars, os
from loader import load

old_file = "data_dump/Well Water Depth.csv"
new_file = "data_dump/cleaned_metro_data.csv"

db_name = "kwb"
schema = "monitoring_well_data"
table_name = "metro_water_depths_post_2020"
prim_key = "state_well_number, reading_date"

new_col_names = {
    "FacilityName1": "state_well_number",
    "Textbox56": "reading_date",
    "Textbox57": "measurement",
}


def remove_unneeded_data():
    with open(old_file, "r") as file_input:
        with open(new_file, "w") as output:
            for line in file_input:
                if "Delivery" in line:
                    break
                output.write(line)
    return new_file


def transform_data(remove_unneeded_data):
    new_file = remove_unneeded_data()
    polars.read_csv(new_file).drop(["Pool1", "Project1"]).rename(
        new_col_names
    ).drop_nulls(subset="Measurement").write_csv(new_file)


if __name__ == "__main__":

    transform_data(remove_unneeded_data)

    load(db_name, schema, table_name, new_file, prim_key)

    os.remove(new_file)
