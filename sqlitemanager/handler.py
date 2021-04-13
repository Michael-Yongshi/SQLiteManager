from .database import (
    Database,
    Table,
    Record,
    )
from .helpers import (
    get_localpath,
    check_existance,
)

class SQLiteHandler(object):
    def __init__(self, filename="", path=""):
        
        self.database = None
        if filename != "":
            if path == "":
                path = get_localpath()
            
            self.database_init(filename = filename, path = path)

    def database_init(self, filename, path):
        # try:
        self.database = Database(
            filename=filename,
            path=path,
        )
        
        # except:
        #     print(f"Couldn't initialize database!")

    def database_new(self, filename, path=""):
        """
        initialises database only if it doesnt exist yet
        """
        if path == "":
            path = get_localpath()

        exists = check_existance(filename, path)

        if exists == True:
            print(f"Couldnt create database, database with filename {filename} at directory {path} already exists!")
        else:
            self.database_init(filename=filename, path=path)

    def database_saveas(self, filename, path=""):
        self.database.saveas_database(filename=filename, path=path)

    def database_open(self, filename, path=""):
        """
        initialises database only if it already exists
        """

        if path == "":
            path = get_localpath()

        exists = check_existance(filename, path)

        if exists == True:
            self.database_init(filename=filename, path=path)
            
            # pull all sql tables and records
            self.database_open_tables()

        else:
            print(f"Couldnt open database, database with filename {filename} at directory {path} doesn't exist!")

    def database_close(self):

        if self.database != None:
            self.database.close_database()
            self.database = None
        else:
            print(f"Couldn't close database, not connected to one!")

    def database_delete(self):
        
        if self.database != None:
            self.database.delete_database()
            self.database = None
        else:
            print(f"Couldn't delete database, not connected to one!")

    def database_open_tables(self):
        """
        Refresh all tables
        """

        # clear tables in database variable
        self.database.tables = {}

        # get existing tables names
        tablenames = self.database.read_table_names()
        tablenames.sort()
        # print(f"getting tablenames for database {self.filename}")

        # for every table create table object and add to database variable
        for tablename in tablenames:
            if tablename != 'sqlite_sequence':

                # get metadata for table
                metadata = self.database.read_column_metadata(tablename)
                column_order = metadata["column_order"]
                column_names = metadata["column_names"]
                column_types = self.database.read_column_types(tablename)

                # get records of table
                sqlrecords = self.database.read_records(tablename=tablename, where="")
                records = self.transform_sql_to_record(column_names=column_names, sqlrecords=sqlrecords)

                # create table object and add to tables dict
                tableobject = Table(tablename, column_names, column_types, records)
                self.database.tables.update({tableobject.name: tableobject})

    def table_create(self, tablename, record_name="", column_names=[], column_types=[], column_placements=[], defaults=[]):
        """
        By default a table will have
        - ordering: to denote a desired order of the records, by default mirrors id column
        - name: a readable reference of this record, preferably will be unique, but is not mandatory

        so put only extra columns on top of these in the column names with a desired type. 
        If they are not given at all the table will have only the columns:
        - id
        - ordering
        - name
        """

        column_names = ["ordering", "name"] + column_names
        column_types = ["INTEGER", "VARCHAR(255)"] + column_types

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

    def table_sync(self, tablename):
        
        table = self.database.tables[tablename]
        sqlrecords = self.database.read_records(tablename=tablename, columns=table.column_names)
        table.records = self.transform_sql_to_record(column_names=table.column_names, sqlrecords=sqlrecords)

    def table_create_add_records(self, tablename, recordsvalues):
        """
        create records from given values and immediately add records in one go to specified table
        """

        records = self.records_create(tablename, recordsvalues)
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
        crossref = self.crossref_get_one(
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

    def record_create(self, tablename, values=[], recordarray=[]):
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

        # if nothing is given, fill the values with default values
        else:
            recordarray = table.defaults

        record = Record(
            table.column_names,
            recordarray,
        )
        # print(f"created record with recordarray {record.recordarray}")
        return record

    def records_create(self, tablename, recordsvalues):
        """
        creates multiple draft records that can still be 
        manipulated before making it definitive

        can be added to the table through
        table_add_records method
        """

        records = []
        for values in recordsvalues:
            records += [self.record_create(tablename, values)]

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