import mysql.connector


'''
Config for incoming and outgoing data

'''

# input settings
inputFile = 'inputFiles/activities.csv'
inputFileClean = 'inputFiles/activities_clean.csv'


inputPersons = 'inputFiles/persons.csv'


# Output Settings
outputFile = 'outputFiles/inserts.sql'


# Connections settings

# Supported connections Mysql

connection = mysql.connector.connect(host='localhost',
                                     database='cycling',
                                     user='root',
                                     password='asus8296')

# schema settings
mysqlQuotesNeeded = ['char',
                     'varchar',
                     'binary',
                     'varbinary'
                     'text',
                     'date',
                     'datetime',
                     'time',
                     'timestamp',
                     'year'
                     ]

cyclingMetrics = {
    'database': 'cycling',
    'tableName': 'metrics',
    'columns': [
        {'columnName': 'activity_type', 'data_type': 'varchar'},
        {'columnName': 'date', 'data_type': 'datetime'},
        {'columnName': 'favorite', 'data_type': 'varchar'},
        {'columnName': 'title', 'data_type': 'varchar'},
        {'columnName': 'distance', 'data_type': 'decimal'},
        {'columnName': 'calories', 'data_type': 'decimal'},
        {'columnName': 'time', 'data_type': 'time'},
        {'columnName': 'avg_speed', 'data_type': 'decimal'},
        {'columnName': 'max_speed', 'data_type': 'decimal'},
        {'columnName': 'elev_gain', 'data_type': 'decimal'},
        {'columnName': 'elev_loss', 'data_type': 'decimal'},
        {'columnName': 'avg_stride_length', 'data_type': 'decimal'},
        {'columnName': 'avg_vertical_ratio', 'data_type': 'decimal'},
        {'columnName': 'avg_vertical_oscillation', 'data_type': 'decimal'},
        {'columnName': 'training_stress_score', 'data_type': 'decimal'},
        {'columnName': 'grit', 'data_type': 'decimal'},
        {'columnName': 'flow', 'data_type': 'decimal'},
        {'columnName': 'bottom_time', 'data_type': 'time'},
        {'columnName': 'min_temp', 'data_type': 'decimal'},
        {'columnName': 'surface_interval', 'data_type': 'time'},
        {'columnName': 'decompression', 'data_type': 'varchar'},
        {'columnName': 'best_lap_time', 'data_type': 'time'},
        {'columnName': 'number_of_runs', 'data_type': 'int'},
        {'columnName': 'max_temp', 'data_type': 'decimal'},
        {'columnName': 'metric_metric_id', 'data_type': 'int'}
                ]
}

cyclingPersons = {'database': 'cycling',
                  'tableName': 'persons',
                  'columns': [{'columnName': 'first_name', 'position': 1, 'data_type': 'varchar'},
                              {'columnName': 'last_name', 'position': 2, 'data_type': 'varchar'},
                              {'columnName': 'email_address', 'position': 3, 'data_type': 'varchar'},
                              {'columnName': 'metric_id', 'position': 5, 'data_type': 'int'}
                              ]
                  }
