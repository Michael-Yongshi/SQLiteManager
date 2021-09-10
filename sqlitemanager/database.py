import os
import datetime

import sqlite3
from sqlite3 import Error

from .helpers import (
    saveas_file,
    check_existance,
)

class Database(object):
    def __init__(self, filename, path):

        self.filename = filename
        self.path = path

        self.connection = None
        self.tables = {}

        existance = check_existance(path=path, filename=filename)
        self.connect_database()

        if existance == True:
            print(f"Database with path {path} and filename {filename} already exists, connection opened to existing database")

        else:
            print(f"Database with path {path} and filename {filename} could not be found, connection opened to new database")

    def delete_database(self):

        self.connection.close()

        completepath = os.path.join(self.path, self.filename + ".sqlite")
        os.remove(completepath)

        print(f"Database deleted!")

    def saveas_database(self, filename, path):

        existance = check_existance(path=path, filename=filename)

        if existance == True:
            print(f"Database with path {path} and filename {filename} already exists, saving canceled!")

        else:
            print(f"Database with path {path} and filename {filename} is free, saving database to the new file")

            saveas_file(
                srcfile = self.filename, 
                dstfile = filename,
                srcpath = self.path,
                dstpath = path,
                )

            new_database = Database(
                filename=filename, 
                path=path,
            )

            return new_database

    def connect_database(self):

        try:
            destination = os.path.join(self.path, f"{self.filename}.sqlite")

            # check if directory already exists, if not create it
            if not os.path.exists(self.path):
                os.makedirs(self.path)

            self.connection = sqlite3.connect(destination)
            print("Connection to SQLite DB successful")

        except Error as e:

            print(f"The error '{e}' occurred")

    def close_database(self):
        self.connection.close()

    def delete_table(self, table):

        query = f"DROP TABLE {table.name}"
        self.execute_query(query)

        del self.tables[table.name]

        print(f"table {table.name} deleted and removed from table list!")

    def delete_records(self, table, records):

        parameters = []
        for record in records:
            parameters += [record.primarykey]

        placeholders = ', '.join('?' for _ in parameters)

        query = f"DELETE FROM {table.name} WHERE id IN ({placeholders})"

        self.execute_parameterised_query(query, parameters)

    def get_table(self, tablename):

        for table in self.tables:
            if table.name == tablename:
                retrieved_table = table
                break

        return retrieved_table

    def execute_query(self, query):
        cursor = self.connection.cursor()

        try:
            # print(f"--------------------\n{query}\n")
            cursor.execute(query)
            self.connection.commit()
            # print("Success!\n--------------------")

            return cursor

        except Error as e:
            print(f"The error '{e}' occurred")

    def execute_parameterised_query(self, query, parameters):
        """
        build a parameterised query:
        for a parameter list of 3 length like below
        -parameters = [1,2,3]
        -placeholders = ', '.join('?' for _ in parameters)
        this results in '?, ?, ?'

        meaning
        for each (_ denotes an unused variable) in parameters, join the strings ('?') with a comma and a space (', ') in order to not have to remove a trailing comma

        then merge with query
        -query= 'SELECT name FROM students WHERE id IN (%s)' % placeholders

        meaning
        this replaces the "%s with our placeholders ('?, ?, ?' in our case)
        """

        cursor = self.connection.cursor()

        try:
            # print(f"--------------------query\n{query}\n")
            # print(f"--------------------parameters\n{parameters}\n")
            cursor.execute(query, parameters)
            self.connection.commit()
            # print("Success!\n--------------------")

            return cursor

        except Error as e:
            print(f"The error '{e}' occurred")

    def read_table_names(self):

        query = f"SELECT name FROM sqlite_master WHERE type='table';"
        
        cursor = self.execute_query(query=query)
        data = cursor.fetchall()
        # print(tables)

        tables = []
        for datapoint in data:
            tables += [datapoint[0]]

        return tables

    def read_column_names(self, table):

        query = f"SELECT * FROM {table};"
        
        cursor = self.execute_query(query=query)
        description = cursor.description
        # print(description)

        # print(description)
        columns = []
        for record in description:
            # print(record[0])
            columns += [record[0]]
        
        # print(columns)
        return columns

    def read_column_metadata(self, table):
        
        # print(table)
        cursor = self.execute_query(f'PRAGMA table_info({table})')
        data = cursor.fetchall()

        column_order = []
        column_names = []
        column_types = []

        for datapoint in data:
            # print(f"{datapoint[0]} {datapoint[1]} {datapoint[2]}")
            column_order += [datapoint[0]]
            column_names += [datapoint[1]]
            column_types += [datapoint[2]]

        metadata = {
            "column_order": column_order,
            "column_names": column_names,
            "column_types": column_types,
        }
        # print(metadata)

        return metadata

    def read_column_types(self, table):

        cursor = self.execute_query(f'PRAGMA table_info({table})')
        data = cursor.fetchall()

        columns = []
        for datapoint in data:
            # print(f"{datapoint[0]} {datapoint[1]} {datapoint[2]}")
            columns += [datapoint[2]]

        # print(columns)
        return columns

    def read_records(self, tablename, columns=[], where = []):

        if columns == []:
            column_line = "*"

        else:
            column_line = ', '.join(columns)
        
        parameters = tuple()

        # where can be collected as [[column name, [values]], [column name2, [values2]]]
        # print(f"where {where}")
        if where == []:
            whereline = ""

        else:
            whereline = "WHERE "
            for statement in where:
                parameters += tuple(statement[1])
                # print(f"statement {statement}")
                # print(f"statement0 {statement[0]}")
                # print(f"statement1 {statement[1]}")
                whereline += f"{statement[0]}"
                whereline += " IN ("
                whereline += ', '.join('?' for _ in statement[1])
                whereline += ') AND '
            whereline = whereline[:-5]
            # print(f"whereline {whereline}")
        # print(f"parameters = {parameters}")

        query = f"SELECT {column_line} from {tablename} {whereline}"

        cursor = self.execute_parameterised_query(query, parameters)
        records = self.get_records_array(cursor.fetchall())

        # print(f"sqlrecords {records}")
        return records

    def create_table(self, name, record_name="", column_names = [], column_types = [], column_placements=[], defaults=[]):
        """
        collects input of table name and column information
        builds a single query and 
        forwards to execute_query
        """

        # add the primary key
        column_names = ["id"] + column_names
        column_types = ["INTEGER PRIMARY KEY AUTOINCREMENT"] + column_types

        columns = []
        # enumerate over column names and types
        for index, column_name in enumerate(column_names):
            columns += [f"{column_name} {column_types[index]}"]
        # print(f"create table columns {columns}")

        # transform variables to string format
        valuetext = ',\n'.join(columns)

        # create variables text
        query = f"CREATE TABLE IF NOT EXISTS {name} (\n{valuetext}\n);"
        self.execute_query(query)

        tableobject = Table(
            name = name,
            record_name=record_name,
            column_names = column_names,
            column_types = column_types,
            column_placements=[],
            defaults = [],
        )

        self.tables.update({tableobject.name: tableobject})
        return tableobject

    def create_records(self, tablename, column_names, valuepairs):
        
        # print(f"create records database with tablename {tablename}, columns {column_names} and valuepairs {valuepairs}")

        # transform column names to a string
        column_text = ', '.join(column_names)

        # create placeholders
        placeholders = ""
        parameters = ()
        for valuepair in valuepairs:
            valuepair_parameters = tuple(valuepair)
            parameters += valuepair_parameters
            valuepair_placeholders = '(' + ','.join('?' for value in valuepair) + '),\n'
            placeholders += valuepair_placeholders
        placeholders = placeholders[:-2]
        # print(f"placeholders = {placeholders}")
        # print(f"parameters = {parameters}")

        query = f"INSERT INTO {tablename}\n({column_text})\nVALUES\n{placeholders}\n;"
        self.execute_parameterised_query(query, parameters)

    def add_records(self, table, records):

        values = []
        for record in records:
            values += [record.values]

        self.create_records(
            tablename = table.name,
            column_names = table.column_names[1:],
            valuepairs = values,
        )

    def update_records(self, tablename, valuepairs, where):

        parameters = tuple()

        # create set_placeholders
        set_placeholders = ""
        for valuepair in valuepairs:
            parameters += tuple([valuepair[1]])
            set_placeholders += valuepair[0] + ' = ?, '
        set_placeholders = set_placeholders[:-2]
        # print(f"set_placeholders = {set_placeholders}")
        # print(f"parameters = {parameters}")

        # create where_placeholders
        where_placeholders = ""
        for statement in where:
            parameters += tuple(statement[1])
            where_placeholders += statement[0] + ' = ? AND '
        where_placeholders = where_placeholders[:-5]
        # print(f"where_placeholders = {where_placeholders}")
        # print(f"parameters = {parameters}")

        query = f"UPDATE {tablename} SET\n{set_placeholders}\nWHERE\n{where_placeholders}\n;"
        self.execute_parameterised_query(query, parameters)

    def get_records_array(self, sqlrecords):

        recordarrays = []

        for sqlrecord in sqlrecords:
            recordarray = []

            for value in sqlrecord:
                recordarray += [value]

            recordarrays += [recordarray]

        return recordarrays

    def get_max_row(self, tablename):

        cursor = self.execute_query(f"SELECT COUNT(id) FROM {tablename}")
        lastrow = cursor.fetchall()[0][0]
        if lastrow == None:
            lastrow = 0

        return lastrow

    def get_max_columncontent(self, table, column):

        query = f"SELECT MAX({column}) FROM {table}"

        cursor = self.execute_query(query)
        max_columncontent = cursor.fetchall()
        if max_columncontent[0][0] == None:
            max_columncontent = [(0,)]

        return max_columncontent[0][0]


class Table(object):
    def __init__(self, name, column_names, column_types, records = (), column_placements = [], defaults = [], record_name = ""):
        super().__init__()

        # set table and record names
        self.name = name
        if record_name == "":
            self.record_name = self.name[:-1]
        else:
            self.record_name = record_name

        # set column names and types
        self.column_names = column_names
        self.column_types = column_types

        self.set_defaults(defaults)
        self.set_column_placements(column_placements)

        self.records = records

    def set_defaults(self, defaults):
        if defaults != []:
            self.defaults = [-1] + defaults
        else:
            self.defaults = []

            self.defaults += [-1]
            for index, value in enumerate(self.column_types[1:]):
                ctype = value.split(' ', 1)[0].upper()

                if ctype == "INTEGER":
                    default = [0]
                elif ctype == "BOOL":
                    default = [False]
                elif ctype == "DATE":
                    default = [datetime.date.today]
                else:
                    default = [""]

                self.defaults += default

            # print(f"defaults set are {self.defaults}")
        
    def set_column_placements(self, column_placements):

        if column_placements != []:
            id_placement = [0,0,1,1]
            self.column_placements = [id_placement] + column_placements

        else:
            self.column_placements = []

            for index, value in enumerate(self.column_names):
                indexconfig = [index,0,1,1]
                self.column_placements += [indexconfig]

        # print(f"column_placements set are {self.column_placements}")


class Record(object):
    def __init__(self, column_names, recordarray):
        super().__init__()

        """
        Primarykey: 
        primary key of this record

        Recordarray: 
        array of values
        including the primary key

        Recordpairs: 
        array of column - value pairs
        including the primary key column
        
        Values: 
        array of all values
        excluding the primary key
        
        Valuepairs: 
        array of column - value pairs
        excluding the primary key column

        With Record.get_dict() command you get the record in dictionary format where
        you can search easily based on column name
        """

        self.primarykey = recordarray[0]
        self.name = ""

        self.recordarray = recordarray
        self.values = recordarray[1:]
        self.setrecordpairs(column_names)
        self.setvaluepairs(column_names)
        self.setrecorddict(column_names)

    def setrecordpairs(self, column_names):
        self.recordpairs = []
        # print(f"Record setting column names {column_names}")
        # print(f"Record setting record array {self.recordarray}")
        for index, name in enumerate(column_names):
            
            recordpair = [name, self.recordarray[index]]
            self.recordpairs += [recordpair]

            if name == "name":
                self.name = self.recordarray[index]
        # print(f"set recordpairs {self.recordpairs}")

    def setvaluepairs(self, column_names):
        self.valuepairs = []
        for index, name in enumerate(column_names[1:]):
            valuepair = [name, self.recordarray[1:][index]]
            self.valuepairs += [valuepair]
        # print(f"set valuepairs {self.valuepairs}")

    def setrecorddict(self, column_names):
        self.recorddict = {}
        for index, name in enumerate(column_names[1:]):
            self.recorddict.update({name: self.recordarray[1:][index]})
        # print(f"set recorddict {self.recorddict}")

    def get_column_value(self, column_name):
        """
        method to easily retrieve a value for a specific column
        """

        # loop over all records valuepairs
        for valuepair in self.valuepairs:
            
            # if column name is found return the value
            if valuepair[0] == column_name:

                column_value = valuepair[1]
                return column_value

        print("Column not found")
        return "Column not found"