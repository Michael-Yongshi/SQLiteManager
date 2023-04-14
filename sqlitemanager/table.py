import datetime

class Table(object):
    def __init__(self, name, column_names, column_types, records = (), column_placements = [], defaults = [], record_name = ""):
        super().__init__()

        # set table name
        self.name = name

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
