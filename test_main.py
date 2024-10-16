"""
Test goes here
"""

import subprocess


def test_extract():
    """tests extract()"""
    result = subprocess.run(
        ["python", "main.py", "extract"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Extracting data..." in result.stdout


def test_transform_load():
    """tests transfrom_load"""
    result = subprocess.run(
        ["python", "main.py", "transform_load"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert "Transforming data..." in result.stdout


def test_general_query():
    """tests general_query"""
    result = subprocess.run(
        [
            "python",
            "main.py",
            "general_query",
            """
            SELECT t1.country, t1.fertility_rate, t1.viral,
                AVG(t1.debt) as avg_debt,
                COUNT(*) as total_countries
            FROM default.der41_wdi1 t1
            JOIN default.der41_wdi2 t2 ON t1.id = t2.id
            GROUP BY t1.country, t1.fertility_rate, t1.viral
            ORDER BY total_countries DESC
            LIMIT 10
            """,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


if __name__ == "__main__":
    test_extract()
    test_transform_load()
    test_general_query()
