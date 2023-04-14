import logging

import os

import sqlite3
from sqlite3 import Error

from .table import Table
from .record import Record
from .helpers import saveas_file


class SQLiteHandler(object):
    def __init__(self, path="", filename="", extension=""):

    def table_create(self, tablename, record_name="", column_dict={}, column_names=[], column_types=[], column_placements=[], defaults=[]):
        """

        column names and types can be given in a dict of form
        {<column name>: <column_type>}
        These will convert automatically to the right arrays for certain keywords (not caps sensitive)
        and can transform these common type terms, like int, integer, str, string, text, character, date, dt, time, year
        Anyforeign keys have to be put exactly as necessary, as these are not caught

        If they are not given at all the table will have only the columns:
        - id
        """

        if column_dict != {}:
            column_names = []
            column_types = []
            # column_defaults = []

            for key in column_dict:

                column_type_transform = column_dict[key].upper()
                if column_type_transform in('INT', 'INTEGER', 'NUMBER'):
                    column_type = "INTEGER"
                elif column_type_transform in('STR', 'STRING', 'TXT', 'NUMBER'):
                    column_type = "TEXT"
                elif column_type_transform in('DATE', 'DT', 'TIME', 'YEAR'):
                    column_type = "DATE"
                else:
                    # otherwise keep exactly the same
                    column_type = column_dict[key]

                column_names += [key]
                column_types += [column_type]

        table = self.database.create_table(
                name=tablename,
                record_name=record_name,
                column_names = column_names,
                column_types = column_types,
                column_placements = column_placements,
                defaults = defaults,
            )
        
        return table

    def table_delete(self, tablename):

        table = self.database.tables[tablename]
        self.database.delete_table(table=table)

        print(f"table {tablename} deleted")

    def load_tables(self):
        """
        Load tables
        """

        # clear tables in database variable
        self.tables = {}

        # get existing tables names
        tablenames = self.read_table_names()
        tablenames.sort()
        logging.info(f"getting tablenames for database {self.filename}")

        # for every table create table object and add to database variable
        for tablename in tablenames:
            # get metadata for table
            metadata = self.read_column_metadata(tablename)

            column_order = metadata["column_order"]
            column_names = metadata["column_names"]
            column_types = self.read_column_types(tablename)

            # get records of table
            sqlrecords = self.read_records(tablename=tablename, where="")
            records = self.transform_sql_to_record(column_names=column_names, sqlrecords=sqlrecords)

            # create table object and add to tables dict
            tableobject = Table(tablename, column_names, column_types, records)
            self.tables.update({tableobject.name: tableobject})

    def table_sync(self, tablename):
        
        table = self.tables[tablename]
        sqlrecords = self.read_records(tablename=tablename, columns=table.column_names)
        table.records = self.transform_sql_to_record(column_names=table.column_names, sqlrecords=sqlrecords)

    def table_create_add_records(self, tablename, recordsvalues=[], recorddicts=[]):
        """
        create records from given values and immediately add records in one go to specified table
        """

        if recorddicts != []:
            records = self.records_create(tablename, recorddicts=recorddicts)

        elif recordsvalues != []:
            records = self.records_create(tablename, recordsvalues=recordsvalues)
        
        self.table_add_records(tablename, records)

        return records

    def table_add_records(self, tablename, records):

        table = self.database.tables[tablename]

        # get current last row
        lastrow = self.table_max_row(tablename)

        # add the records to the table in the database
        self.database.add_records(
            table,
            records,
        )

        # refresh tableobjects records
        self.table_sync(tablename)

        # get last row after update
        newlastrow = self.table_max_row(tablename)

        # get all the just created rows (in an array lastrow of 1 is the 2nd record, so using lastrow number gets the record after the actual last row)
        records = table.records[lastrow:newlastrow]

        # print(f"Added records:")
        # print_records(records)

        return records

    def table_update_records(self, tablename, valuepairs, where):
        """
        update specific records of the specified table

        if where is an integer or array of integers, it will update those specified rows based on primary key.
        otherwise a valuepair is expected in the form 
        [<columnname>, [<values]]
        """

        where = self.transform_where(where=where)

        records_to_be_updated = self.table_read_records(tablename, where=where)

        ids_to_be_updated = []
        for record in records_to_be_updated:
            ids_to_be_updated += [record.primarykey]
        ids_to_be_updated = [["id", ids_to_be_updated]]

        # the actual updating
        self.database.update_records(
            tablename=tablename, 
            valuepairs = valuepairs,
            where=where,
        )

        # refresh table records
        self.table_sync(tablename)

        print(f"ids to be updated {ids_to_be_updated}")
        # read updated records from primary key list
        records = self.table_read_records(tablename, where=ids_to_be_updated)

        return records

    def table_read_records(self, tablename, where=[]):

        table = self.database.tables[tablename]

        if where == []:
            records = table.records
        else:
            sqlrecords = self.database.read_records(tablename=tablename, columns=table.column_names, where=where)
            records = self.transform_sql_to_record(column_names=table.column_names, sqlrecords=sqlrecords)
            # print(f"{self.name} records retrieved: {self.records}")

        return records

    def table_get_foreign_table(self, tablename, column):
        """
        for a particular tableobject and column,
        checks if column is a foreign key
        if so finds the table it points to
        """

        table = self.database.tables[tablename]

        # check if the column points to a foreign key
        columnindex = table.column_names.index(column)
        split = table.column_types[columnindex].split(' ', 3)
        if len(split) != 3:
            return
        if split[1].upper() != "REFERENCES":
            return

        # get the reference to the foreign table
        foreign_table_name = split[2].split('(',1)[0]
        foreign_table = self.database.tables[foreign_table_name]
        # print(f"foreign table {foreign_table}")

        return foreign_table

    def table_get_foreign_records(self, tablename, column, where=[]):
        """
        for a particular tableobject and column,
        checks if column is a foreign key
        if so finds the table it points to and gets the records.
        if a where is given, only give the foreign value(s) that are linked to the found rows
        records will be returned as an array of record objects
        """

        foreign_table = self.table_get_foreign_table(tablename=tablename, column=column)
        
        # get the records from the foreign table
        records = self.table_read_records(foreign_table.name)
        # print(f"record objects of foreign table {records}")

        # get the foreign keys to filter on
        # records = self.table_read_records(tablename=tablename, where=where)
        # foreign_keys = []
        # for record in records:
        #     foreign_keys += [record.recorddict[column]]

        return records

    def table_delete_records(self, tablename, where=[]):

        table = self.database.tables[tablename]

        where = self.transform_where(where=where)

        records = self.table_read_records(tablename=tablename, where=where)
        self.database.delete_records(table=table, records=records)

        self.table_sync(tablename)

    def table_max_row(self, tablename):
        lastrow = self.database.get_max_row(tablename)
        return lastrow

    def crossref_create(self, tablename1, tablename2):
        """
        Creates a crossreference table for tables with given table names.
        Next to the columns that contain the foreign keys, a column for a description is made for additional info.
        """

        crossref_table = self.table_create(
            tablename = f"CROSSREF_{tablename1}_{tablename2}",
            column_names=[f"{tablename1}_id", f"{tablename2}_id", "description"],
            column_types=[f"INTEGER REFERENCES {tablename1}(id)", f"INTEGER REFERENCES {tablename2}(id)", "TEXT"],
        )

        return crossref_table

    def crossref_get(self, tablename1, tablename2):
        """
        Gets a crossreference table for tables with given table names if it exists.
        will check first the following combination
        'CROSSREF_tablename1_tablename2'
        and if table != found will check
        'CROSSREF_tablename2_tablename1'

        Returns the table if it is found
        """

        crossref_nominal = 'CROSSREF_' + tablename1 + '_' + tablename2
        crossref_inverse = 'CROSSREF_' + tablename2 + '_' + tablename1
        # print(crossref_nominal)
        # print(crossref_inverse)

        try:
            crossref = self.database.get_table(crossref_nominal)
            # print(f"crossref nominal found with {crossref}")

        except:
            crossref = self.database.get_table(crossref_inverse)
            # print(f"crossref inverse found with {crossref}")
        
        return crossref

    def crossref_get_all(self, tablename):
        """
        loops over all the tables in the database.
        If they start with CROSSREF, checks if the name contains this tablename.
        if they do, find also the table it points to
        get an array of all the found tables and return it
        """

        tables = []

        for key in self.database.tables:
            if ("CROSSREF" in key) and (tablename in key):
                key = key.replace("CROSSREF", '')
                key = key.replace(tablename, '')
                key = key.replace('_', '')

                table = self.database.tables[key]
                tables += [table]

        return tables       

#Depreciated
    def crossref_get_one(self, tablename1, tablename2):
        """
        loops over all the tables in the database.
        If they start with CROSSREF and contain both the table names, find the table it points to
        return the found table
        """

        for tablename in self.database.tables:
            if ("CROSSREF" in tablename) and (tablename1 in tablename) and (tablename2 in tablename):
                table = self.database.tables[tablename]
                break

        return table

    def crossref_add_record(self, tablename1, tablename2, where1, where2, description=""):
        """
        adds a crossreference between tables 1 and 2 for the rows given by the where statements.
        Creates links for any combination of the found rows of table1 and table 2. So if the where statements point to multiple rows,
        like 1-3 in table 1 and 5-8 in table 2, it loops over all possible combinations:
        1 - 5
        1 - 6
        1 - 7
        1 - 8

        2 - 5
        2 - 6
        2 - 7
        2 - 8

        3 - 5
        3 - 6
        3 - 7
        3 - 8
        """

        # get the cross reference table
        crossref = self.crossref_get(
            tablename1 = tablename1,
            tablename2 = tablename2, 
            )

        # print(f"where1 is {where1}")
        # get record primary keys / row id's
        if isinstance(where1[0], int):
            rowids1 = where1
        else:
            rowids1 = []
            records = self.table_read_records(
                tablename = tablename1,
                where = where1,
            )
            for record in records:
                rowids1 += [record.primarykey]
        # print(f"rowids1 is {rowids1}")

        # print(f"where2 is {where2}")
        if isinstance(where2[0], int):
            rowids2 = where2
        else:
            rowids2 = []
            records = self.table_read_records(
                tablename = tablename2,
                where = where2,
            )
            for record in records:
                rowids2 += [record.primarykey]
        # print(f"rowids2 is {rowids2}")

        # determine if tables need to be switched places
        index1 = crossref.name.find(tablename1)
        index2 = crossref.name.find(tablename2)

        if (index1 == -1) or (index2 == -1):
            print(f"table1 {tablename1} or table2 {tablename2} not found")
            return

        # switch columns
        values1 = rowids1 if index1 < index2 else rowids2
        values2 = rowids2 if index1 < index2 else rowids1

        lastrow = self.table_max_row(tablename=crossref.name)

        records = []
        for value1 in values1:
            for value2 in values2:
                # default description
                if description == "":
                    description = f"Cross referenced {where1} to {where2}"

                record = self.record_create(
                    tablename=crossref.name,
                    values=[
                        lastrow + 1, f"{value1}-{value2}", value1, value2, description
                    ]
                )
                records += [record]

        self.table_add_records(crossref.name, records)

    def crossref_read_records(self, tablename1, tablename2):

        table = self.crossref_get_one(tablename1=tablename1, tablename2=tablename2)
        records = self.table_read_records(table.name)

        return records

    def crossref_read_record(self, tablename1, tablename2, primarykey):
        """
        reads the crossreference table and retrieves only elements for the rowid given of table1
        """

        table = self.crossref_get_one(tablename1, tablename2)
        # print(table.name)
        records = self.table_read_records(table.name)
        
        # print(records)

        records_found = []
        for record in records:
            for valuepair in record.valuepairs:
                if valuepair[0] == tablename1 + "_id":
                    if valuepair[1] == primarykey:
                        records_found += [record]
                        break

        return records_found

    def record_create(self, tablename, values=[], recordarray=[], recorddict={}):
        """
        creates a draft record that can still be 
        manipulated before making it definitive
        if values are empty, takes default values of the columns

        can be added to the table through
        table_add_records method
        """

        table = self.database.tables[tablename]

        # if recordarray is given, take it as is
        if recordarray != []:
            pass

        # if instead only values is given, add a new id in front and take that as recordarray
        # (this will be a new record in the database and has therefore no id yet)
        elif values != []:
            recordarray = [-1] + values

        elif recorddict != {}:
            recordarray = [-1]
            for column_name in table.column_names:
                try:
                    values += [recorddict[column_name]]
                except:
                    values += []

        # if nothing is given, fill the values with default values
        else:
            recordarray = table.defaults

        record = Record(
            table.column_names,
            recordarray,
        )
        # print(f"created record with recordarray {record.recordarray}")
        return record

    def records_create(self, tablename, recordsvalues =[], recorddicts = []):
        """
        creates multiple draft records that can still be 
        manipulated before making it definitive

        can be added to the table through
        table_add_records method
        """

        records = []

        if recordsvalues != []:

            for values in recordsvalues:
                records += [self.record_create(tablename, values=values)]

        elif recorddicts != []:
            for recorddict in recorddicts:
                records += [self.record_create(tablename, recorddict=recorddict)]

        return records

    def transform_sql_to_record(self, column_names, sqlrecords):
        """
        uses table object as input, as its used privately and not made to be called directly by using application
        """

        # print(sqlrecords)

        records = []
        for sqlrecord in sqlrecords:

            recordarray = []
            for value in sqlrecord:
                recordarray += [value]

            recordobject = Record(
                column_names=column_names, 
                recordarray=recordarray
                )
            # print(f"Transformed Record object with recordarray: {recordobject.recordarray}")

            records += [recordobject]

        return records

    def transform_where(self, where):
        
        # if where is an integer, create where to delete a that record with this primary key (id column)
        if isinstance(where, int):
            where = [["id", [where]]]
        
        # if where is an array of integers, create where to delete those records with these primary keys (id column)
        elif isinstance(where[0], int):
            where = [["id", where]]

        # otherwise, no transform is needed
        return where

    def check_existance(self):

        if os.path.exists(self.get_complete_path()):
            return True
        else:
            return False

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
        
        retrieved_table = self.tables[tablename]
        print(retrieved_table)

        return retrieved_table

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