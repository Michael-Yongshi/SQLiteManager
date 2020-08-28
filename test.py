from sqlitemanager.handler import (
    SQLiteHandler,
)

if __name__ == '__main__':

    # opening a database
    handler = SQLiteHandler()
    handler.database_open(filename="science")
    handler.database_delete()
    handler.database_new(filename="science")

    # adding a table
    table_scientists = handler.database.create_table(
        name="scientists",
        column_names = ["ordering", "name", "age", "nobelprizewinner"],
        column_types = ["INTEGER", "Text", "Integer", "Bool"],
    )
    print(f"Database contains {handler.database.tables}")

    # adding multiple records
    records = []
    records += [handler.record_create(
        table_scientists,
        [1, "Hawking", 68, True],
        )]
    records += [handler.record_create(
        table_scientists,
        [2, "Edison's child said \"Apple!\"", 20, True],
        )]
    handler.table_add_records(table_scientists, records)
    print(f"creating multiple records")
    print_records(table_scientists.records)
    
    # adding single records
    records = []
    records += [handler.record_create(
        table_scientists,
        [3, "Einstein", 100, False],
        )]
    handler.table_add_records(table_scientists, records)
    print(f"creating single records")
    print_records(table_scientists.records)

    # adding multiple records
    records = []
    records += [handler.record_create(
        table_scientists,
        [4, "Rosenburg", 78, False],
        )]
    records += [handler.record_create(
        table_scientists,
        [5, "Neil dGrasse Tyson", 57, True],
        )]
    handler.table_add_records(table_scientists, records)
    print(f"creating multiple records")
    print_records(table_scientists.records)

    # conditional read with where statement
    where = [["nobelprizewinner", [True]]]
    records = handler.table_read_records(table=table_scientists, where=where)
    print(f"read where")
    print_records(records)

    # update with where statement
    valuepairs = [["nobelprizewinner", False]]
    where = [["nobelprizewinner", [True]], ["name", ["Hawking"]]]
    handler.table_update_records(table=table_scientists, valuepairs=valuepairs, where=where)
    records = handler.table_read_records(table=table_scientists)
    print(f"update true to false")
    print_records(records)

    valuepairs = [["name", "Neil de'Grasse Tyson"], ["age", 40]]
    rowid = 5
    handler.table_update_records(table=table_scientists, valuepairs=valuepairs, where=rowid)
    records = handler.table_read_records(table=table_scientists)
    print(f"update record 'id = 5'")
    print_records(records)

    # get records without table object, but with tablename
    records = handler.table_read_records(table_scientists)
    print(f"read all records")
    print_records(records)

    # adding a table
    table_nobel = handler.database.create_table(
        name = "nobelprizes",
        column_names=["ordering", "name", "description"],
        column_types=["INTEGER", "VARCHAR(255)", "TEXT"],
    )
    
    # adding multiple records in one go
    records = handler.records_create(
        table_nobel,
            [
            [1, "Peace", "Peace nobel prize"],
            [2, "Economy", "Economy nobel prize"],
            [3, "Physics", "Physics nobel prize"],
            [4, "Sociology", "Sociology nobel prize"],
            ]
        )
    handler.table_add_records(table_nobel, records)
    print(f"creating multiple records")
    print_records(table_nobel.records)

    # adding a crossref table
    crossref_table = handler.crossref_create(
        tablename1="scientists",
        tablename2="nobelprizes",
    )
    print(f"Database contains {handler.database.tables}")

    # get crossreferences
    crossreferences = handler.crossref_get_all("scientists")
    print(f"Table {table_scientists.name} has links to {crossreferences}")

    # adding a crossref
    handler.crossref_add_record(
        table1=table_scientists,
        table2=table_nobel,
        where1=[
            ["name", ["Hawking"]]
            ],
        where2=[
            ["name", ["Economy"]]
            ],
    )

    # adding a crossref
    handler.crossref_add_record(
        table1=table_scientists,
        table2=table_nobel,
        where1=[
            ["name", ["Einstein"]]
            ],
        where2=[
            ["name", ["Physics", "Peace"]]
            ],
        description="Einstein wins both peace and physics nobel prizes"
    )

    # reading crossreferences
    records = handler.crossref_read_records(
        tablename1="scientists",
        tablename2="nobelprizes",
        )
    print_records(records)

    # records = reltbl.readRecords()
    # print(f"read all records")
    # print_records(records)

    # records = reltbl.readForeignValues('charid1')

    # db.saveas_database(filename="backup")
    # db.close_database()

    # filename = "science"

    # db = Database(filename=filename)

    # teachertbl = db.create_table(
    #     name="teachers",
    #     column_names = ["ordering", "name", "age", "active"],
    #     column_types = ["INTEGER", "Text", "Integer", "Bool"],
    # )

    # db.delete_table(teachertbl)

    # table_scientists = db.get_table("scientists")

    # where = [["nobelprizewinner", [True]]]
    # records = table_scientists.readRecords(where=where)

    # table_scientists.deleteRecords(records)

    # for table in db.tables:
    #     print(f"printing for table: {table.name}")
    #     print_records(table.records)