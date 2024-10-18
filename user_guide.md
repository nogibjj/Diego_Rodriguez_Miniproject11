# User guide

## Overview

Here are the different steps required to execute the SQL queries using Databricks platform from Command line using `setup.py` .

## Preparation
First is necessary to set up the tools. We start by installing the packages:
```bash
python setup.py install
```
or: 
```bash
python setup.py develop
```

## Running Queries
1. Extract the information from the World Development Indicator CSV ans save it under `data` directory:
```bash
etl_query extract
```
2. Transform the `csv` file into a `db` using databricks:
```bash
etl_query transform_load
```
3. Perform querys into Databricks to process the `db`:
```bash
etl_query general_query <query>
```
if not query is provided the standard query provided under `query_log` is executed.