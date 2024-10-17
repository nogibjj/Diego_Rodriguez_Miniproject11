## Diego_Rodriguez_Miniproject6
`Add picture of of CI/CD`

### File Structure
```
Diego_Rodriguez_Miniproject6/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/hello.yml
├── .gitignore
├── data/
│   ├── wdi1.csv
│   └── wdi2.csv
├── mylib/
│   ├── extract.py
│   ├── query.py
│   └── transform_load.py
├── .gitignore
├── main.py
├── Makefile
├── query_log.md
├── README.md
├── requirements.txt
├── test_main.py
└── wdi.db
```

### Purpose of project
The purpose of this project is to build an ETL-Query pipeline. I use World Bank, World Development Indicators dataset to extract it into a local csv file, transform the csv file by cleaning it, loading it into a .db ready for Complex SQL query using Databricks.

### Database Connection
1. Under `mylib/` directory `extract.py` extract raw data from an online source. 
2. Under `mylib/` directory `transform_and_load.py` clean and transform raw data from `csv` to `db` and builds connections to databricks. This is possible by using `Github Secrets`, connected through `setup.py`, and running CI/CD operations with the `env:` option on the `yml` file. 

### Complex Query
Under `mylib/` directory `query.py` performs the complex Query operation. In this case: 

`Add picture of query`

This SQL query retrieves data by joining two tables, `default.der41_wdi1` and `default.der41_wdi2`, based on a common `id` field. It selects the country, fertility rate, and viral attribute from t1, along with the average debt for each country and a count of the total number of records (countries) in each group. The results are grouped by country, fertility rate, and viral attribute, with the count of records determining the group size. The query then sorts the results in descending order by the total number of countries and limits the output to the top 10 groups. 

The current query and response of Databricks is registered on `query_log.md` by the `main.py`. 

### Results

`Add picture of of results`

### Testing

`Add picture of of testing`

