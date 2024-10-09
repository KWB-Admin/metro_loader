# Description

This is an ETL pipeline for taking `metro` data and loading it into the KWB data warehouse.

Once data is downloaded from `metro` in `.csv` format, the files are dumped in the `data_dump` folder. From here, the ETL is ran following morning via Task Scheduler and the Power Shell script.

# Requirements

Currently this relies on one package, `kwb_loader`, which will install three dependies:


1. `psychopg2`
2. `polars`
3. `numpy`

Please see the requirements.txt file for specific versions.