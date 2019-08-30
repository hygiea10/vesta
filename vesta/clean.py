"""
This module contains functions that performs a task to clean data.
The goal of each function is to perform one clean up action for
every row in the input file or perform an action that contributes
to the cleaning of the data.

"""

import csv
import config


def append_default_val(csv_file_input, csv_file_output, default_value):
    """
    Takes a csv file and adds default_value to the end of each record and
    writes records back to csv file csv_file_output
    :param csv_file_input:
    :param csv_file_output:
    :param default_value:
    :return:
    """
    # variables to hold the data read in from file
    data = []
    with open(csv_file_input, 'r') as read_file:
        csv_reader = csv.reader(read_file, delimiter=',')
        for row in csv_reader:
            row.append(default_value)
            data.append(row)

    # newline parameter needed to fix writing extra blink
    with open(csv_file_output, 'w', newline='') as write_file:
        writer = csv.writer(write_file)
        writer.writerows(data)

    # close files
    read_file.close()
    write_file.close()
    return


def none_null_redirect(csv_file_input, tuple_or_list='list', none_null=None):
    """
    Data Cleaning takes '' and replaces it with the None object or NULL String

    :param csv_file_input:
    :param tuple_or_list must pass 'tuple' if a list of tuples is desired
    :param none_null
    :return: returns a list or tuple depending on the value in tuple_or_list
    """
    with open(csv_file_input, 'r') as read_file:
        # variables to hold the data read in from file
        data = []
        data_holder = []

        csv_reader = csv.reader(read_file, delimiter=',')
        for row in csv_reader:
            # list comprehension format for if else statement [f(x) if condition else g(x) for x in sequence]
            # if the value is '' then replace it with the None object to represent NULL when inserted into the
            # database

            if none_null == 'null':
                [data_holder.append('NULL') if value == '' else data_holder.append(value) for value in row]
            else:
                [data_holder.append(None) if value == '' else data_holder.append(value) for value in row]

            # converts data type to tuple if tuple_or_list =

            if tuple_or_list == 'tuple':
                data.append(tuple(data_holder))
            else:
                data.append(data_holder)
            data_holder = []

    # close files
    read_file.close()
    return data


def quotes_needed(config_dictionary):
    """
    Just determines base on data type if quotes are needed or not
    :param config_dictionary: must supply a list of data types
    :return: True or False
    """
    # the following data types needs quotes char', 'varchar', 'binary', 'varbinary', 'text',
    # 'date', 'datetime', 'time', 'timestamp', 'year
    # all these value are store in config module

    if config_dictionary in config.mysqlQuotesNeeded:
        return True
    else:
        return False


def remove_character(csv_file_input, csv_file_output,  schema_obj, char_to_remove):
    """
    Data Cleaning Function that removes any character from numeric data types only
    numerics values are determined by quotes_needed() function, returning false
    implies value must be numeric

    :param csv_file_input:
    :param csv_file_output:
    :param schema_obj:
    :param char_to_remove:
    :return:
    """

    with open(csv_file_input, 'r') as read_file:
        # variables to hold the data read in from file
        data = []
        data_holder = []

        csv_reader = csv.reader(read_file, delimiter=',')
        for row in csv_reader:
            for idx, value in enumerate(row):
                # Determines if the value is numeric
                if quotes_needed(schema_obj['columns'][idx]['data_type']) == 0:
                    value = value.replace(char_to_remove, '')
                    data_holder.append(value)
                else:
                    data_holder.append(value)
            # Append new values to the list data
            data.append(data_holder)
            data_holder = []

    # newline parameter needed to fix writing extra blink
    with open(csv_file_output, 'w', newline='') as write_file:
        writer = csv.writer(write_file)
        writer.writerows(data)

    # close files
    read_file.close()
    write_file.close()
    return


def remove_header(csv_file_input, csv_file_output):
    """
    Data Cleaning Function that removes header from csv file
    :param csv_file_input:
    :param csv_file_output:
    :return:
    """
    with open(csv_file_input, 'r') as read_file:
        csv_reader = csv.reader(read_file, delimiter=',')
        # list comprehension that returns all the rows in csv file except the first one
        data = [value for idx, value in enumerate(csv_reader) if idx > 0]

    # newline parameter needed to fix writing extra blink to file
    with open(csv_file_output, 'w', newline='') as write_file:
        writer = csv.writer(write_file)
        # writes list back to csv file
        writer.writerows(data)

    # close files
    read_file.close()
    write_file.close()
    return


def tag_replacer(text_with_tag, tag, replace_string):
    """
    takes wherever it finds a tag and replaces it with the value in replace_string
    :param text_with_tag:
    :param tag:
    :param replace_string:
    :return: text with replaced string
    """
    text_with_tag = text_with_tag.replace(tag, replace_string)
    return text_with_tag
