"""
Vesta ETL Engine
This contain core logic that will take input data and produce
target output data.

Description: This primary functionality for Vesta.

Take a CSV file and does clean up operations to perform inserts to a database
The schema for the database is defined in config.py module.

In addition the code takes the CSV file and converts statement into an insert
statement and sends output to a txt file.



"""

import clean
import config
import mysql_interact as interact


# check to confirm database is connected
interact.database_checker(config.connection)

# Functions that performs data cleaning process

# metrics table
clean.remove_header(config.inputFile, config.inputFileClean)
clean.remove_character(config.inputFileClean, config.inputFileClean, config.cyclingMetrics, '--')
clean.remove_character(config.inputFileClean, config.inputFileClean, config.cyclingMetrics, ',')
clean.append_default_val(config.inputFileClean, config.inputFileClean, 1)


print("Cleaning Completed")


# Functions that support MySQL functionality

# Performs inserts
#interact.insert_data(config.connection, config.cyclingPersons, config.inputPersons)
#interact.insert_data(config.connection, config.cyclingMetrics, config.inputFileClean)


# performs deletes
#interact.data_delete(config.connection, config.cyclingMetrics, '1=1')
#interact.data_delete(config.connection, config.cyclingPersons, '1=1')

interact.close_database(config.connection)


# Writes inserts statements to file
interact.write_insert_statement(config.inputFileClean, config.outputFile, config.cyclingMetrics)

