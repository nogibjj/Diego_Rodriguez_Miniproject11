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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```


```sql
SELECT t1.country, t1.fertility_rate, t1.viral, AVG(t1.debt) as avg_debt, COUNT(*) as total_countries FROM default.der41_wdi1 t1 JOIN default.der41_wdi2 t2 ON t1.id = t2.id GROUP BY t1.country, t1.fertility_rate, t1.viral ORDER BY total_countries DESC LIMIT 10
```

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

```sql
SELECT t1.country, t1.fertility_rate, t1.viral, AVG(t1.debt) as avg_debt, COUNT(*) as total_countries FROM default.der41_wdi1 t1 JOIN default.der41_wdi2 t2 ON t1.id = t2.id GROUP BY t1.country, t1.fertility_rate, t1.viral ORDER BY total_countries DESC LIMIT 10
```

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

```sql
SELECT t1.country, t1.fertility_rate, t1.viral, AVG(t1.debt) as avg_debt, COUNT(*) as total_countries FROM default.der41_wdi1 t1 JOIN default.der41_wdi2 t2 ON t1.id = t2.id GROUP BY t1.country, t1.fertility_rate, t1.viral ORDER BY total_countries DESC LIMIT 10
```

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

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

```response from databricks
[Row(country='Bangladesh', fertility_rate=85, viral=15, avg_debt=4.0, total_countries=1), Row(country='Bolivia', fertility_rate=69, viral=81, avg_debt=4.0, total_countries=1), Row(country='Guam', fertility_rate=34, viral=0, avg_debt=0.0, total_countries=1), Row(country='Belize', fertility_rate=65, viral=59, avg_debt=0.0, total_countries=1), Row(country='Czech Republic', fertility_rate=10, viral=0, avg_debt=0.0, total_countries=1), Row(country='Kazakhstan', fertility_rate=29, viral=95, avg_debt=0.0, total_countries=1), Row(country='Belgium', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Barbados', fertility_rate=41, viral=0, avg_debt=0.0, total_countries=1), Row(country='Libya', fertility_rate=5, viral=0, avg_debt=0.0, total_countries=1), Row(country='Cyprus', fertility_rate=4, viral=0, avg_debt=0.0, total_countries=1)]
```

