import clean
import templates.templates as tmpl
import sys
import config
import csv
import os


def close_database(db_connection):
    """
    clean up connections and close connections

    :param db_connection:
    :return:
    """

    if db_connection.is_connected():
        db_connection.close()
        print("MySQL connection is closed")
        return


def column_list(schema_obj):
    """
    This function returns a list of columns found in schemaObj['columnName'] found in the config file
    the string that is returned is input of the insert statement
    :param schema_obj:
    :return: column_lister

    """
    # Declare variable to store Columns
    column_lister = ''

    # create list for columns for insert statement
    for column in schema_obj:
        column_lister = column_lister + column['columnName'] + ','
    # last comma not needed therefore it will be removed
    column_lister = column_lister.rstrip(',')

    return column_lister


def database_checker(db_connection):
    """
    Checks to determine if a database connection is established
    :param db_connection:
    :return:
    """
    if db_connection.is_connected():
        db_info = db_connection.get_server_info()
        print("Connected to MySQL Server version ", db_info)
        cursor = db_connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Your connected to database: ", record[0])
    else:
        print("No Connection was established aborting....")
        sys.exit()
    return


def data_delete(db_connection, schema_obj, where_clause):
    """
    create delete statement
    :param db_connection:
    :param schema_obj:
    :param where_clause
    :return:
    """

    sql = tmpl.delete
    sql = clean.tag_replacer(sql, '{{table_name}}', schema_obj['tableName'])
    sql = clean.tag_replacer(sql, '{{where_condition}}', where_clause)

    mydb = db_connection
    mycursor = mydb.cursor()
    print (sql)
    mycursor.execute(sql)

    mydb.commit()
    mycursor.close()


    print('Data Successfully Deleted for: ' + schema_obj['tableName'])

    return


def data_truncate(db_connection, schema_obj):
    """
    create truncate statement
    :param db_connection:
    :param schema_obj:
    :return:
    """

    sql = tmpl.truncate
    sql = clean.tag_replacer(sql, '{{table_name}}', schema_obj['tableName'])

    mydb = db_connection
    mycursor = mydb.cursor()

    mycursor.execute(sql)
    mycursor.close()

    print('Data Successfully Truncated for: ' + schema_obj['tableName'])

    return


def insert_data(db_connection, schema_obj, csv_file):
    """
    creates the insert statement and using a cursor execute inserts to mySQL database

    :param db_connection: data connection defined in config module
    :param schema_obj:
    :param csv_file:
    :return:
    """
    # initiate connection
    mydb = db_connection

    # get template for insert statement and replace tags
    sql = tmpl.insert
    sql = clean.tag_replacer(sql, '{{table_name}}', schema_obj['tableName'])
    sql = clean.tag_replacer(sql, '{{column_list}}', column_list(schema_obj['columns']))
    sql = clean.tag_replacer(sql, '{{column_values}}', placeholder_gen(schema_obj['columns']))

    # build list of tuples
    val = clean.none_null_redirect(csv_file, 'tuple')

    # create cursor object
    mycursor = mydb.cursor()
    # perform bulk inserts
    mycursor.executemany(sql, val)

    if mycursor.rowcount == 1:
        print(mycursor.rowcount, 'record inserted into table ' + schema_obj['tableName'])
    else:
        print(mycursor.rowcount, 'records inserted into table ' + schema_obj['tableName'])
    # commit to database and close cursor
    mydb.commit()
    mycursor.close()

    return


def placeholder_gen(schema_obj):
    """
    this function will return %s, %s, %s, %s based on the number
    of columns in schem_obj

    :param schema_obj: config.schemaObj1
    :return: placer
    """
    # use character multiplication to build string with correct number of place holders
    placer = str('%s,' * len(schema_obj))
    # last comma not needed therefore remove it
    placer = placer.rstrip(',')
    print('Number of columns found in: ' + str(len(schema_obj)))
    return placer


def insert_statement_gen(schema_obj, column_values):
    """
    Generates one insert statement based on column_values argument
    :param schema_obj:
    :param column_values: expecting string
    :return:
    """
    statement = ''

    # get template for insert statement and replace tags
    sql = tmpl.insert
    sql = clean.tag_replacer(sql, '{{table_name}}', schema_obj['tableName'])
    sql = clean.tag_replacer(sql, '{{column_list}}', column_list(schema_obj['columns']))

    for idx, value in enumerate(column_values):
        if value == 'NULL':
            # if value is NULL then no quotes are needed
            statement = statement + value + ','
        elif clean.quotes_needed(schema_obj['columns'][idx]['data_type']) == 1:
            # add quotes to correct data types. if there is a single quote in a
            # string another is needed for mySql insert to work correctly
            value = value.replace('\'', '\'\'')
            statement = statement + '\'' + value + '\'' + ','
        else:
            statement = statement + value + ','
    statement = statement.rstrip(',')

    sql = clean.tag_replacer(sql, '{{column_values}}', statement)

    return sql


def write_insert_statement(csv_file_input, file_output, schema_obj):
    """
    Takes each column value and creates full insert statement and
    stores all statements to a list. Then writes each item in list
    to file_output

    :param csv_file_input:
    :param file_output:
    :param schema_obj:
    :return:
    """

    val = clean.none_null_redirect(csv_file_input, 'list', 'null')
    data = [insert_statement_gen(schema_obj, row) for row in val]

    with open(file_output, 'w') as file:
        [file.write(value + ';\n') for value in data]
        file.close()
    # Fix formatting
    file_output = file_output.replace('/', '\\')
    print('New File Created: ' + os.getcwd() + '\\' + file_output)
    return
