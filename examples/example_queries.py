"""
Example SQL queries and their conversions between different dialects.

This module demonstrates how to use the SQL Converter package with
various types of SQL queries.
"""

from sql_converter import convert_sql

def mysql_to_postgresql():
    """Examples of converting MySQL queries to PostgreSQL."""
    
    examples = [
        # Example 1: Simple SELECT with LIMIT
        {
            "title": "Simple SELECT with LIMIT",
            "source_sql": "SELECT id, name FROM users LIMIT 10",
            "source_dialect": "mysql",
            "target_dialect": "postgresql"
        },
        
        # Example 2: Using MySQL-specific functions
        {
            "title": "Query with MySQL date functions",
            "source_sql": """
            SELECT 
                DATE_FORMAT(created_at, '%Y-%m-%d') AS date,
                COUNT(*) AS count
            FROM orders
            WHERE created_at >= NOW() - INTERVAL 30 DAY
            GROUP BY DATE_FORMAT(created_at, '%Y-%m-%d')
            """,
            "source_dialect": "mysql",
            "target_dialect": "postgresql"
        },
        
        # Example 3: JOINs and aggregations
        {
            "title": "Query with JOINs and aggregations",
            "source_sql": """
            SELECT 
                u.id,
                u.name,
                COUNT(o.id) AS order_count,
                IFNULL(SUM(o.amount), 0) AS total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.created_at >= '2020-01-01'
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 0
            ORDER BY total_spent DESC
            LIMIT 100
            """,
            "source_dialect": "mysql",
            "target_dialect": "postgresql"
        }
    ]
    
    # Convert and print each example
    for example in examples:
        print(f"\n--- {example['title']} ---")
        print("\nSource SQL (MySQL):")
        print(example['source_sql'])
        
        converted = convert_sql(
            example['source_sql'], 
            example['source_dialect'], 
            example['target_dialect']
        )
        
        print("\nConverted SQL (PostgreSQL):")
        print(converted)
        print("\n" + "-" * 80)

def oracle_to_pyspark():
    """Examples of converting Oracle queries to PySpark."""
    
    examples = [
        # Example 1: Oracle pagination with ROWNUM
        {
            "title": "Oracle pagination with ROWNUM",
            "source_sql": """
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    SELECT * FROM employees ORDER BY hire_date DESC
                ) a
                WHERE ROWNUM <= 30
            ) WHERE rnum > 20
            """,
            "source_dialect": "oracle",
            "target_dialect": "pyspark"
        },
        
        # Example 2: Oracle date functions
        {
            "title": "Query with Oracle date functions",
            "source_sql": """
            SELECT 
                employee_id,
                first_name,
                last_name,
                TO_CHAR(hire_date, 'YYYY-MM-DD') AS hire_date
            FROM employees
            WHERE hire_date > ADD_MONTHS(SYSDATE, -12)
            """,
            "source_dialect": "oracle",
            "target_dialect": "pyspark"
        },
        
        # Example 3: Oracle NVL and string concatenation
        {
            "title": "Query with Oracle NVL and string concatenation",
            "source_sql": """
            SELECT 
                employee_id,
                first_name || ' ' || last_name AS full_name,
                NVL(department_id, 0) AS dept_id,
                NVL(commission_pct, 0) * salary AS commission
            FROM employees
            WHERE department_id IS NOT NULL
            """,
            "source_dialect": "oracle",
            "target_dialect": "pyspark"
        }
    ]
    
    # Convert and print each example
    for example in examples:
        print(f"\n--- {example['title']} ---")
        print("\nSource SQL (Oracle):")
        print(example['source_sql'])
        
        converted = convert_sql(
            example['source_sql'], 
            example['source_dialect'], 
            example['target_dialect']
        )
        
        print("\nConverted SQL (PySpark):")
        print(converted)
        print("\n" + "-" * 80)

def postgresql_to_mysql():
    """Examples of converting PostgreSQL queries to MySQL."""
    
    examples = [
        # Example 1: Window functions
        {
            "title": "PostgreSQL window functions",
            "source_sql": """
            SELECT 
                product_id,
                product_name,
                price,
                category_id,
                ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC) AS price_rank
            FROM products
            """,
            "source_dialect": "postgresql",
            "target_dialect": "mysql"
        },
        
        # Example 2: PostgreSQL array operations
        {
            "title": "PostgreSQL array operations",
            "source_sql": """
            SELECT 
                user_id,
                username,
                ARRAY_LENGTH(interests, 1) AS interest_count
            FROM users
            WHERE 'hiking' = ANY(interests)
            """,
            "source_dialect": "postgresql",
            "target_dialect": "mysql"
        },
        
        # Example 3: Common Table Expressions (CTE)
        {
            "title": "PostgreSQL Common Table Expressions",
            "source_sql": """
            WITH active_users AS (
                SELECT 
                    user_id,
                    username,
                    email
                FROM users
                WHERE last_login_at >= CURRENT_DATE - INTERVAL '30 days'
            )
            SELECT 
                au.user_id,
                au.username,
                COUNT(o.order_id) AS order_count
            FROM active_users au
            LEFT JOIN orders o ON au.user_id = o.user_id
            GROUP BY au.user_id, au.username
            ORDER BY order_count DESC
            LIMIT 100
            """,
            "source_dialect": "postgresql",
            "target_dialect": "mysql"
        }
    ]
    
    # Convert and print each example
    for example in examples:
        print(f"\n--- {example['title']} ---")
        print("\nSource SQL (PostgreSQL):")
        print(example['source_sql'])
        
        converted = convert_sql(
            example['source_sql'], 
            example['source_dialect'], 
            example['target_dialect']
        )
        
        print("\nConverted SQL (MySQL):")
        print(converted)
        print("\n" + "-" * 80)

if __name__ == "__main__":
    print("===== MySQL to PostgreSQL Examples =====")
    mysql_to_postgresql()
    
    print("\n\n===== Oracle to PySpark Examples =====")
    oracle_to_pyspark()
    
    print("\n\n===== PostgreSQL to MySQL Examples =====")
    postgresql_to_mysql()
