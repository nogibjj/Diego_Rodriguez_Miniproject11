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


