# SQL Converter

A Python package that converts SQL queries between different database dialects (Oracle, PySpark, MySQL, PostgreSQL).

## Project Structure

This project is organized into two versions:

1. **Standalone Python Package**: Located in the `sql_converter_pkg` directory, this is a standalone installable Python package that can be used directly in code without any web interface.

2. **Web Application**: The main project directory contains a Flask-based web application that provides a user-friendly interface for the SQL converter functionality.

## Features

- Parse SQL queries from various formats (Oracle, PySpark, MySQL, PostgreSQL)
- Convert SQL queries between different formats
- Handle basic SQL syntax differences between dialects
- Support common SQL operations (SELECT, INSERT, UPDATE, DELETE)
- Process SQL with tables, joins, where clauses, and aggregations
- Return properly formatted SQL in the target dialect
- Command-line interface for direct conversion
- Python API for integration into other applications
- Support for batch processing of multiple queries
- Oracle to PySpark special function conversions with over 50 function mappings
- SQL query cleanup (comments, hints, whitespace)
- Old-style joins to ANSI JOIN conversion
- SQL dependency analyzer to determine correct table creation order

## Standalone Package Installation

To install the standalone package:

```bash
cd sql_converter_pkg
pip install -e .
