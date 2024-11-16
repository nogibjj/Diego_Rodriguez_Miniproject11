## Diego_Rodriguez_Miniproject11
[![CI/CD run](https://github.com/nogibjj/Diego_Rodriguez_Miniproject6/actions/workflows/hello.yml/badge.svg)](https://github.com/nogibjj/Diego_Rodriguez_Miniproject6/actions/workflows/hello.yml)

### File Structure
```
Diego_Rodriguez_Miniproject11/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/hello.yml
├── Databricks_test/extract_test.py
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
├── README.md
├── requirements.txt
├── test_main.py
└── wdi.db
```

### Purpose of project
The purpose of this project is to build an ETL-Query pipeline using Spark and Databricks workflows capabilities. I use World Bank, World Development Indicators dataset to extract it into a cloud hosted csv file, transform the csv file by cleaning it, loading it into a .db ready for query using Databricks.This is further automated by using workflows through `job_run.py`. 

### Database Connection
1. Under `mylib/` directory `extract.py` extract raw data from an online source. 
2. Under `mylib/` directory `transform_and_load.py` clean and transform raw data from `csv` to `db` and builds connections to databricks. 
3. Under `mylib/` directory `query.py` perform a simple query to the  `db` file.

### Exploring Databricks.

1. Saving files into `DBFS` with `extract()` from `extract.py`:

![Screenshot 2024-11-16 at 6 15 45 PM](https://github.com/user-attachments/assets/0c7087b5-e85e-4366-bfaf-4b75f0ca17c1)

2. Checking `DBFS` results: 

![image](https://github.com/user-attachments/assets/003d4065-953a-4006-89b2-daa5056b270d)

3. Transforming the `csv` to `db` with `load()` from `transform_and_load.py`:

![image](https://github.com/user-attachments/assets/61667f4c-f08f-443a-afcf-bdf2ea43621b)

4. Performing a simple query to the cloud dataset. (`query_transform()` from `query.py`is ready to be adapted to more complex queries):

![image](https://github.com/user-attachments/assets/6564b2c8-ba9d-48f6-b472-6ccb3851a604).

5. The pipeline for automated workflows actions in databricks from `job_run.py`:

![image](https://github.com/user-attachments/assets/f7edb6e9-45ff-45c1-b28e-f0dde4fdb56c)


### Exploring Databricks - Workflows
1. The Dasboard usage:

![image](https://github.com/user-attachments/assets/d507a5f3-f67a-4b42-83b8-347d8bbc13cc)

2. The tasks executed:
   
![image](https://github.com/user-attachments/assets/10a31b27-a8f1-4f18-96b1-dce423caba93)




