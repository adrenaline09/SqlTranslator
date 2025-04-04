"""
Setup script for the sql_converter package.
"""

from setuptools import setup, find_packages

setup(
    name="sql_converter",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "sqlparse>=0.4.3",
    ],
    entry_points={
        "console_scripts": [
            "sql-converter=sql_converter.cli:main",
        ],
    },
    author="SQL Converter Team",
    author_email="info@sqlconverter.example.com",
    description="A tool to convert SQL queries between different database dialects",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/example/sql_converter",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Database Administrators",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.9",
)
