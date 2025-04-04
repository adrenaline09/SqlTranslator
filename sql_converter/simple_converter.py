"""
Enhanced SQL Converter module.

An implementation of the SQL converter using regular expressions with
advanced features like query cleanup, comment removal, and join conversions.
"""

import re
import logging
import sqlglot
from typing import Dict, List, Pattern, Union, Callable, Optional, Match, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Oracle to PySpark function mappings
ORACLE_TO_PYSPARK_FUNCTIONS = {
    # Date/Time functions
    'SYSDATE': 'current_date()',
    'SYSTIMESTAMP': 'current_timestamp()',
    'CURRENT_DATE': 'current_date()',
    'CURRENT_TIMESTAMP': 'current_timestamp()',
    'TO_DATE': 'to_date',
    'TO_CHAR': 'date_format',
    'TRUNC': 'date_trunc',
    'EXTRACT': 'extract',
    'MONTHS_BETWEEN': 'months_between',
    'NEXT_DAY': 'next_day',
    'LAST_DAY': 'last_day',
    'ADD_MONTHS': 'add_months',
    'TO_TIMESTAMP': 'to_timestamp',
    
    # String functions
    'NVL': 'coalesce',
    'NVL2': 'when',   # Requires special handling with CASE
    'DECODE': 'when', # Requires special handling with CASE
    'INSTR': 'instr',
    'LENGTH': 'length',
    'LOWER': 'lower',
    'UPPER': 'upper',
    'LPAD': 'lpad',
    'RPAD': 'rpad',
    'LTRIM': 'ltrim',
    'RTRIM': 'rtrim',
    'REPLACE': 'regexp_replace',
    'SUBSTR': 'substring',
    'TRIM': 'trim',
    'INITCAP': 'initcap',
    
    # Math functions
    'ROUND': 'round',
    'TRUNC': 'truncate', # Note: trunc is overloaded for both date and number
    'MOD': 'pmod',
    'ABS': 'abs',
    'SIGN': 'signum',
    'FLOOR': 'floor',
    'CEIL': 'ceil',
    'POWER': 'pow',
    'SQRT': 'sqrt',
    'EXP': 'exp',
    'LN': 'log',
    
    # Aggregation functions
    'AVG': 'avg',
    'COUNT': 'count',
    'MAX': 'max',
    'MIN': 'min',
    'SUM': 'sum',
    'STDDEV': 'stddev',
    'VARIANCE': 'variance',
    
    # Analytic functions
    'RANK': 'rank',
    'DENSE_RANK': 'dense_rank',
    'ROW_NUMBER': 'row_number',
    'LEAD': 'lead',
    'LAG': 'lag',
    'FIRST_VALUE': 'first',
    'LAST_VALUE': 'last',
    'PERCENTILE_CONT': 'percentile',
}

# Date format mappings
DATE_FORMAT_MAPS = {
    'mysql_to_postgresql': {
        '%Y': 'YYYY',
        '%m': 'MM',
        '%d': 'DD',
        '%H': 'HH24',
        '%i': 'MI',
        '%s': 'SS',
    },
    'postgresql_to_mysql': {
        'YYYY': '%Y',
        'MM': '%m',
        'DD': '%d',
        'HH24': '%H',
        'MI': '%i',
        'SS': '%s',
    },
    'oracle_to_pyspark': {
        'YYYY': 'yyyy',
        'MM': 'MM',
        'DD': 'dd',
        'HH24': 'HH',
        'HH': 'hh',
        'MI': 'mm',
        'SS': 'ss',
    },
}

# ========= SQL CLEANUP FUNCTIONS =========

def clean_sql(sql: str, custom_removals: Optional[List[str]] = None) -> str:
    """
    Clean SQL query by removing comments, unnecessary whitespace, junk characters,
    and normalizing syntax. Prepares SQL for more reliable parsing with sqlglot.
    
    Args:
        sql (str): The SQL query to clean
        custom_removals (List[str], optional): List of characters or words to be removed from the SQL query.
            Can include both exact strings or regex patterns. Defaults to None.
        
    Returns:
        str: The cleaned SQL query
    """
    # Remove null bytes and other control characters
    sql = sql.replace('\0', '')  # Remove null bytes
    
    # Remove junk characters that may have been introduced from copy/paste operations or encoding issues
    sql = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', sql)
    
    # Apply custom removals if provided
    if custom_removals:
        for item in custom_removals:
            try:
                # Try to treat it as a regex pattern first
                sql = re.sub(item, '', sql, flags=re.IGNORECASE)
            except re.error:
                # If not a valid regex, treat as literal string
                sql = sql.replace(item, '')
    
    # Remove single-line comments
    sql = re.sub(r'--.*?(?:\n|$)', ' ', sql, flags=re.MULTILINE)
    
    # Remove multi-line comments (including nested comments)
    # This pattern handles nested comments better than the simple one
    sql = re.sub(r'/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/', ' ', sql)
    
    # Remove Oracle hints like /*+ ... */
    sql = re.sub(r'/\*\+.*?\*/', ' ', sql, flags=re.DOTALL)
    
    # Remove query hints and directives for various dialects
    sql = re.sub(r'\/\*.*?NO_INDEX.*?\*\/', ' ', sql, flags=re.IGNORECASE | re.DOTALL)
    sql = re.sub(r'\/\*.*?PARALLEL.*?\*\/', ' ', sql, flags=re.IGNORECASE | re.DOTALL)
    sql = re.sub(r'\/\*.*?FULL.*?\*\/', ' ', sql, flags=re.IGNORECASE | re.DOTALL)
    
    # Remove EXPLAIN PLAN commands which are dialect-specific
    sql = re.sub(r'^\s*EXPLAIN\s+PLAN\s+.*?$', '', sql, flags=re.MULTILINE | re.IGNORECASE)
    
    # Remove SET commands often used in Oracle/SQL Server
    sql = re.sub(r'^\s*SET\s+.*?;', '', sql, flags=re.MULTILINE | re.IGNORECASE)
    
    # Normalize newlines and tabs to spaces first
    sql = re.sub(r'[\t\n\r]+', ' ', sql)
    
    # Normalize excessive whitespace
    sql = re.sub(r'\s{2,}', ' ', sql)
    
    # Normalize parentheses spacing for better parser compatibility
    sql = re.sub(r'\(\s+', '(', sql)
    sql = re.sub(r'\s+\)', ')', sql)
    
    # Add space after commas for better readability
    sql = re.sub(r',(?=\S)', ', ', sql)
    
    # Normalize quotes - careful handling to maintain quoted strings integrity
    # Only normalize identifiers, not string literals
    
    # Ensure all keywords have correct spacing
    # This makes it easier for sqlglot to properly parse the SQL
    for keyword in ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 
                    'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL', 'UNION',
                    'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']:
        # Pattern matches keyword as a whole word preceded by space/start-of-string and ensures a space after it
        # Using word boundaries \b to prevent matching keywords inside identifiers (e.g., join_parameter)
        pattern = r'(^|\s)\b' + keyword + r'\b(?=\S)'
        replacement = r'\1' + keyword + r' '
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    
    return sql.strip()

def convert_old_style_joins(sql: str) -> str:
    """
    Convert old-style Oracle comma joins to ANSI JOIN syntax.
    
    Args:
        sql (str): The SQL query with old-style joins
        
    Returns:
        str: The SQL query with modern ANSI JOIN syntax
    """
    # This is a complex transformation that requires parsing the query
    # Here's a simplified approach that handles basic cases
    
    # First, let's identify if this is likely a SELECT statement
    if not re.match(r'^\s*SELECT', sql, re.IGNORECASE):
        return sql  # Not a SELECT, so return as is
    
    # Extract the FROM clause (simplified)
    from_match = re.search(r'FROM\s+(.*?)(?:WHERE|GROUP BY|HAVING|ORDER BY|LIMIT|$)', 
                          sql, re.IGNORECASE | re.DOTALL)
    
    if not from_match:
        return sql  # No FROM clause found
    
    from_clause = from_match.group(1).strip()
    
    # Check if there are commas in the FROM clause (potential old-style joins)
    if ',' not in from_clause:
        return sql  # No commas, likely not old-style joins
    
    # Check if there are already JOIN keywords
    if re.search(r'\bJOIN\b', from_clause, re.IGNORECASE):
        return sql  # Already has JOIN syntax
    
    # This is where it gets tricky - we need to:
    # 1. Identify the tables in the FROM clause
    # 2. Find the join conditions in the WHERE clause
    # 3. Convert to ANSI JOIN syntax
    
    # Split the FROM clause by commas to get table references
    tables = [t.strip() for t in from_clause.split(',')]
    
    # Extract the WHERE clause
    where_match = re.search(r'WHERE\s+(.*?)(?:GROUP BY|HAVING|ORDER BY|LIMIT|$)', 
                           sql, re.IGNORECASE | re.DOTALL)
    
    if not where_match:
        # No WHERE clause, can't determine join conditions
        # Just return with inner joins but no ON conditions (not ideal)
        base_table = tables[0]
        join_clause = base_table
        for table in tables[1:]:
            join_clause += f" INNER JOIN {table} ON 1=1"
            
        return sql.replace(from_clause, join_clause)
    
    where_clause = where_match.group(1).strip()
    
    # Split WHERE conditions by AND to find potential join conditions
    conditions = re.split(r'\bAND\b', where_clause, flags=re.IGNORECASE)
    
    # Identify join conditions (very simplified - assumes equality joins on single columns)
    join_conditions = []
    remaining_conditions = []
    
    for condition in conditions:
        # Look for patterns like "t1.col = t2.col"
        if re.search(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', condition):
            join_conditions.append(condition.strip())
        else:
            remaining_conditions.append(condition.strip())
    
    # If we didn't find any join conditions, return original SQL
    if not join_conditions:
        return sql
    
    # Create the new FROM clause with ANSI JOIN syntax
    base_table = tables[0]
    join_clause = base_table
    used_tables = {base_table.split(' ')[-1].strip()}  # Handle "schema.table alias"
    
    # Process join conditions
    for join_condition in join_conditions:
        match = re.search(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', join_condition)
        if not match:
            continue
            
        left_table, left_col, right_table, right_col = match.groups()
        
        # Determine which table to join next
        if left_table in used_tables and right_table not in used_tables:
            next_table = right_table
            for table in tables[1:]:
                if right_table in table.split(' '):
                    next_table = table
                    break
            join_clause += f" INNER JOIN {next_table} ON {join_condition}"
            used_tables.add(right_table)
        elif right_table in used_tables and left_table not in used_tables:
            next_table = left_table
            for table in tables[1:]:
                if left_table in table.split(' '):
                    next_table = table
                    break
            join_clause += f" INNER JOIN {next_table} ON {join_condition}"
            used_tables.add(left_table)
    
    # Handle any remaining tables (with cross joins)
    for table in tables[1:]:
        table_name = table.split(' ')[-1].strip()
        if table_name not in used_tables:
            join_clause += f" CROSS JOIN {table}"
            used_tables.add(table_name)
    
    # Replace FROM clause
    result = sql.replace(from_clause, join_clause)
    
    # Update WHERE clause if we moved conditions
    if join_conditions:
        if remaining_conditions:
            new_where = " WHERE " + " AND ".join(remaining_conditions)
            result = re.sub(r'WHERE\s+.*?(?:GROUP BY|HAVING|ORDER BY|LIMIT|$)', 
                           new_where + ' ', result, flags=re.IGNORECASE | re.DOTALL)
        else:
            # Remove the WHERE clause entirely if all conditions were join conditions
            result = re.sub(r'WHERE\s+.*?(?:GROUP BY|HAVING|ORDER BY|LIMIT|$)', 
                           ' ', result, flags=re.IGNORECASE | re.DOTALL)
    
    return result

def _convert_oracle_to_pyspark_nvl2(match: Match) -> str:
    """Convert Oracle NVL2 to PySpark CASE WHEN."""
    expr = match.group(1)
    value_if_not_null = match.group(2)
    value_if_null = match.group(3)
    return f"CASE WHEN {expr} IS NOT NULL THEN {value_if_not_null} ELSE {value_if_null} END"

def _convert_oracle_to_pyspark_decode(match: Match) -> str:
    """Convert Oracle DECODE to PySpark CASE WHEN."""
    parts = match.group(1).split(',')
    if len(parts) < 3:
        return match.group(0)  # Not enough arguments
    
    expr = parts[0].strip()
    result = "CASE"
    
    # Process pairs of comparison value and result
    i = 1
    while i < len(parts) - 1:
        comp_value = parts[i].strip()
        result_value = parts[i+1].strip()
        result += f" WHEN {expr} = {comp_value} THEN {result_value}"
        i += 2
    
    # Add ELSE clause if there's an odd number of remaining arguments
    if i < len(parts):
        result += f" ELSE {parts[i].strip()}"
    
    result += " END"
    return result

def _convert_oracle_to_pyspark_date_format(match: Match) -> str:
    """Convert Oracle TO_CHAR date format to PySpark date_format."""
    column = match.group(1)
    format_str = match.group(2)
    
    # Convert Oracle date format to PySpark date format
    mapping = DATE_FORMAT_MAPS['oracle_to_pyspark']
    for src, tgt in mapping.items():
        format_str = format_str.replace(src, tgt)
    
    return f"date_format({column}, '{format_str}')"

def _convert_oracle_rownum_to_pyspark(sql: str) -> str:
    """Convert Oracle ROWNUM pagination to PySpark limit."""
    # Simple ROWNUM condition
    sql = re.sub(r'ROWNUM\s*<=\s*(\d+)', r'LIMIT \1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'ROWNUM\s*<\s*(\d+)', lambda m: f"LIMIT {int(m.group(1)) - 1}", sql, flags=re.IGNORECASE)
    
    # More complex ROWNUM range condition (simplified approach)
    sql = re.sub(r'ROWNUM\s*>=\s*(\d+)\s+AND\s+ROWNUM\s*<=\s*(\d+)', 
                lambda m: f"LIMIT {int(m.group(2)) - int(m.group(1)) + 1} OFFSET {int(m.group(1)) - 1}", 
                sql, flags=re.IGNORECASE)
    
    return sql

def _convert_oracle_to_pyspark_functions(sql: str) -> str:
    """Replace Oracle function names with their PySpark equivalents."""
    # Process special cases that need custom handling
    # NVL2
    sql = re.sub(r'NVL2\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)', 
                _convert_oracle_to_pyspark_nvl2, sql, flags=re.IGNORECASE)
    
    # DECODE
    sql = re.sub(r'DECODE\s*\(\s*([^)]+)\s*\)', 
                _convert_oracle_to_pyspark_decode, sql, flags=re.IGNORECASE)
    
    # TO_CHAR for dates
    sql = re.sub(r'TO_CHAR\s*\(\s*([^,]+)\s*,\s*[\'"]([^\'"]*)[\'"]\s*\)', 
                _convert_oracle_to_pyspark_date_format, sql, flags=re.IGNORECASE)
    
    # Replace other functions
    for oracle_func, pyspark_func in ORACLE_TO_PYSPARK_FUNCTIONS.items():
        # Only replace whole words with word boundaries
        sql = re.sub(r'\b' + oracle_func + r'\s*\(', pyspark_func + '(', sql, flags=re.IGNORECASE)
    
    return sql

def _convert_oracle_trunc_to_pyspark(match: Match) -> str:
    """Convert Oracle TRUNC function to appropriate PySpark function based on argument types."""
    args = match.group(1)
    
    # Check if it's likely a date truncation (has a date unit as second arg)
    if re.search(r'[\'"]YEAR[\'"]|[\'"]MONTH[\'"]|[\'"]DAY[\'"]|[\'"]HOUR[\'"]', args, re.IGNORECASE):
        # For date truncation, use date_trunc
        # Oracle: TRUNC(date, 'MONTH') -> PySpark: date_trunc('month', date)
        match_parts = re.match(r'([^,]+),\s*[\'"](YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)[\'"]', args, re.IGNORECASE)
        if match_parts:
            date_expr = match_parts.group(1).strip()
            unit = match_parts.group(2).lower()
            return f"date_trunc('{unit}', {date_expr})"
    
    # If no date unit or other case, assume it's numeric truncation
    return f"truncate({args})"

def _convert_date_format(match, source, target):
    """Convert date format string between dialects."""
    column = match.group(1)
    format_str = match.group(2)
    
    mapping_key = f"{source}_to_{target}"
    if mapping_key in DATE_FORMAT_MAPS:
        mapping = DATE_FORMAT_MAPS[mapping_key]
        for src, tgt in mapping.items():
            format_str = format_str.replace(src, tgt)
    
    if target == 'postgresql':
        return f"TO_CHAR({column}, '{format_str}')"
    elif target == 'mysql':
        return f"DATE_FORMAT({column}, '{format_str}')"
    elif target == 'pyspark':
        return f"date_format({column}, '{format_str}')"
    return match.group(0)  # Return original if no conversion

def _mysql_concat_to_postgres(match):
    """Convert MySQL CONCAT to PostgreSQL || operator."""
    args = match.group(1).split(',')
    return ' || '.join(arg.strip() for arg in args)

# Define function replacements for various dialects
REPLACEMENTS = {
    # MySQL to PostgreSQL
    ('mysql', 'postgresql'): [
        # Date/Time functions
        (r'DATE_FORMAT\s*\(\s*([^,]+)\s*,\s*[\'"]([^\'"]*)[\'"]\s*\)', lambda m: _convert_date_format(m, 'mysql', 'postgresql')),
        (r'NOW\(\s*\)', 'CURRENT_TIMESTAMP'),
        (r'CURDATE\(\s*\)', 'CURRENT_DATE'),
        (r'INTERVAL\s+(\d+)\s+DAY', r"INTERVAL '\1 DAY'"),
        # String functions
        (r'CONCAT\s*\(([^)]+)\)', lambda m: _mysql_concat_to_postgres(m)),
        (r'IFNULL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'COALESCE(\1, \2)'),
        # Limit
        (r'LIMIT\s+(\d+)\s*,\s*(\d+)', r'LIMIT \2 OFFSET \1'),
    ],
    
    # PostgreSQL to MySQL
    ('postgresql', 'mysql'): [
        # Date/Time functions
        (r'TO_CHAR\s*\(\s*([^,]+)\s*,\s*[\'"]([^\'"]*)[\'"]\s*\)', lambda m: _convert_date_format(m, 'postgresql', 'mysql')),
        (r'CURRENT_TIMESTAMP', 'NOW()'),
        (r'CURRENT_DATE', 'CURDATE()'),
        (r"INTERVAL\s+[\'\"]((?:\d+\s+)?[^\'\"]+)[\'\"]", r'INTERVAL \1'),
        # String functions
        (r'([\w.]+)\s*\|\|\s*([\w.\'\"]+)', r'CONCAT(\1, \2)'),
        (r'COALESCE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'IFNULL(\1, \2)'),
    ],
    
    # Oracle to PostgreSQL
    ('oracle', 'postgresql'): [
        # Date/Time functions
        (r'SYSDATE', 'CURRENT_DATE'),
        (r'SYSTIMESTAMP', 'CURRENT_TIMESTAMP'),
        (r'ADD_MONTHS\s*\(\s*([^,]+)\s*,\s*(\-?\d+)\s*\)', r"(\1 + INTERVAL '\2 MONTH')"),
        # String functions
        (r'NVL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'COALESCE(\1, \2)'),
        # Pagination
        (r'ROWNUM\s*<=\s*(\d+)', r'LIMIT \1'),
    ],
    
    # Oracle to PySpark - we only define basic patterns here
    # The comprehensive conversion is done by the specialized function
    ('oracle', 'pyspark'): [
        # Date/Time functions
        (r'SYSDATE', 'current_date()'),
        (r'SYSTIMESTAMP', 'current_timestamp()'),
        (r'ADD_MONTHS\s*\(\s*([^,]+)\s*,\s*(\-?\d+)\s*\)', r'add_months(\1, \2)'),
        # String functions
        (r'([\w.]+)\s*\|\|\s*([\w.\'\"]+)', r'concat(\1, \2)'),
        (r'NVL\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)', r'coalesce(\1, \2)'),
    ],
}

def convert_oracle_to_pyspark(sql: str) -> str:
    """
    Specialized conversion from Oracle SQL to PySpark SQL with comprehensive function mapping.
    
    Args:
        sql (str): The Oracle SQL query to convert
        
    Returns:
        str: The converted PySpark SQL query
    """
    # First clean the SQL
    sql = clean_sql(sql)
    
    # Convert old-style joins to ANSI joins
    sql = convert_old_style_joins(sql)
    
    # Handle ROWNUM pagination
    sql = _convert_oracle_rownum_to_pyspark(sql)
    
    # Replace Oracle function calls with PySpark equivalents
    sql = _convert_oracle_to_pyspark_functions(sql)
    
    # Special handling for TRUNC which could be date or number function
    sql = re.sub(r'TRUNC\s*\(\s*([^)]+)\s*\)', _convert_oracle_trunc_to_pyspark, sql, flags=re.IGNORECASE)
    
    # Apply other standard replacements from the pattern list
    for pattern, replacement in REPLACEMENTS[('oracle', 'pyspark')]:
        if callable(replacement):
            # Use custom replacement function
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        else:
            # Use simple string replacement
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    
    return sql

def convert_sql(sql: str, source_dialect: str, target_dialect: str, custom_removals: Optional[List[str]] = None) -> str:
    """
    Convert SQL from one dialect to another with enhanced cleaning and transformation.
    First applies robust SQL cleanup, then uses sqlglot for dialect conversion.
    
    Args:
        sql (str): The SQL query to convert
        source_dialect (str): The source SQL dialect
        target_dialect (str): The target SQL dialect
        custom_removals (List[str], optional): List of characters or words to be removed from the SQL query.
            Can include both exact strings or regex patterns. Defaults to None.
        
    Returns:
        str: The converted SQL query
    """
    # If source and target are the same, return original after cleaning
    if source_dialect.lower() == target_dialect.lower():
        return clean_sql(sql, custom_removals)
    
    # Step 1: Clean SQL first (remove comments, normalize whitespace, remove junk characters)
    sql = clean_sql(sql, custom_removals)
    
    # Step 2: Convert old-style joins to ANSI JOIN syntax (especially important for Oracle)
    if source_dialect.lower() == 'oracle':
        sql = convert_old_style_joins(sql)
    
    # Special case for Oracle to PySpark - use specialized function for complex Oracle-specific transformations
    if source_dialect.lower() == 'oracle' and target_dialect.lower() == 'pyspark':
        sql = convert_oracle_to_pyspark(sql)
    
    try:
        # Step 3: Use sqlglot for the actual dialect conversion
        # Map our dialect names to sqlglot dialect names
        dialect_mapping = {
            'postgresql': 'postgres',
            'mysql': 'mysql',
            'oracle': 'oracle',
            'pyspark': 'spark'
        }
        
        source = dialect_mapping.get(source_dialect.lower(), source_dialect.lower())
        target = dialect_mapping.get(target_dialect.lower(), target_dialect.lower())
        
        # Try to parse and transpile with sqlglot
        try:
            converted_sql = sqlglot.transpile(
                sql, 
                read=source,
                write=target,
                pretty=True
            )[0]
            return converted_sql
        except Exception as e:
            logger.warning(f"sqlglot conversion failed: {e}. Falling back to regex-based conversion.")
            # If sqlglot fails, fall back to our regex-based conversion
    except ImportError:
        logger.warning("sqlglot not available, falling back to regex-based conversion")
    
    # Fall back to regex replacements if sqlglot fails or is not available
    key = (source_dialect.lower(), target_dialect.lower())
    if key not in REPLACEMENTS:
        logger.warning(f"No replacements defined for {source_dialect} to {target_dialect}")
        return sql
    
    replacements = REPLACEMENTS[key]
    
    # Apply each replacement in sequence
    result = sql
    for pattern, replacement in replacements:
        if callable(replacement):
            # Use custom replacement function
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        else:
            # Use simple string replacement
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    
    return result

def batch_convert(sql_queries: List[str], source_dialect: str, target_dialect: str, custom_removals: Optional[List[str]] = None) -> List[str]:
    """
    Convert multiple SQL queries from one dialect to another.
    
    Args:
        sql_queries (List[str]): List of SQL queries to convert
        source_dialect (str): The source SQL dialect
        target_dialect (str): The target SQL dialect
        custom_removals (List[str], optional): List of characters or words to be removed from the SQL queries.
            Can include both exact strings or regex patterns. Defaults to None.
        
    Returns:
        List[str]: List of converted SQL queries
    """
    return [convert_sql(sql, source_dialect, target_dialect, custom_removals) for sql in sql_queries]