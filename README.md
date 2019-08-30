# vesta
ETL Tool

"""
Vesta ETL Engine
This contain core logic that will take input data and produce
target output data.

Description: The primary functionality for Vesta.

Take a CSV file and does clean up operations to perform inserts to a database
The schema for the database is defined in config.py module.

In addition the code takes the CSV file and converts statement into an insert
statement and sends output to a txt file.

The data in the CSV file is a dump of cycling metrics

"""
