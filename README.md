## Diego_Rodriguez_Miniproject6
[![CI/CD run](https://github.com/nogibjj/Diego_Rodriguez_Miniproject6/actions/workflows/hello.yml/badge.svg)](https://github.com/nogibjj/Diego_Rodriguez_Miniproject6/actions/workflows/hello.yml)

### File Structure
```
Diego_Rodriguez_Miniproject6/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/hello.yml
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
├── user_guide.md
└── wdi.db
```

### Purpose of project
The purpose of this project is to build an ETL-Query pipeline. I use World Bank, World Development Indicators dataset to extract it into a local csv file, transform the csv file by cleaning it, loading it into a .db ready for Complex SQL query using Databricks.This is possible by using `Github Secrets`, connected through `setup.py`, and running CI/CD operations with the `env:` option on the `yml` file. For a detailed step by step procedure to execute from the command line please check [here](https://github.com/nogibjj/Diego_Rodriguez_Miniproject6/blob/main/user_guide.md).

### Database Connection
1. Under `mylib/` directory `extract.py` extract raw data from an online source. 
2. Under `mylib/` directory `transform_and_load.py` clean and transform raw data from `csv` to `db` and builds connections to databricks. 

### Complex Query
Under `mylib/` directory `query.py` performs the complex Query operation. In this case: 

```sql

            SELECT t1.country, t1.fertility_rate, t1.viral,
                AVG(t1.debt) as avg_debt,
                COUNT(*) as total_countries
            FROM default.der41_wdi1 t1
            JOIN default.der41_wdi2 t2 ON t1.id = t2.id
            GROUP BY t1.country, t1.fertility_rate, t1.viral
            ORDER BY total_countries DESC
            LIMIT 10
            
```
This SQL query retrieves data by joining two tables, `default.der41_wdi1` and `default.der41_wdi2`, based on a common `id` field. It selects the country, fertility rate, and viral attribute from t1, along with the average debt for each country and a count of the total number of records (countries) in each group. The results are grouped by country, fertility rate, and viral attribute, with the count of records determining the group size. The query then sorts the results in descending order by the total number of countries and limits the output to the top 10 groups. 

The current query and response of Databricks is registered on `query_log.md` by the `main.py`. 

### Results

1. Databricks Response:
<img width="1521" alt="Databricks response" src="https://github.com/user-attachments/assets/ee1da0a4-f5e1-4873-b817-05226cd5856d">

2. Log Output: 
```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

3. SQL Results:
<img width="714" alt="SQL_result" src="https://github.com/user-attachments/assets/f94be0fa-87ba-4868-b58d-e82e08a25bb0">

### Testing
<img width="971" alt="Testing" src="https://github.com/user-attachments/assets/77d6724a-a1fe-4ca0-86b1-5e61a3ff00a5">

