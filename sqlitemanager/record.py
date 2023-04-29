from dataclasses import dataclass

@dataclass
class Record:
    """
    dict: dictionary with key and values

    record.print() will print the record to the terminal and return the string printed
    """

    table: str
    columns: list
    dict: dict

    def create_from_sqlrecord(table_name, column_names, values):

        record_dict = dict(zip(column_names, values))
        record_object = Record(table=table_name, columns=column_names, dict=record_dict)

        return record_object

    def print(self):

        print_string = ""
        for column_name, value in self.dict.items():
            print_string += f"{column_name}: {value}, "
        print_string = print_string[:-2]

        print(print_string)
        return print_string


    # def setvaluepairs(self, column_names):
    #     self.valuepairs = []
    #     for index, name in enumerate(column_names[1:]):
    #         valuepair = [name, self.recordarray[1:][index]]
    #         self.valuepairs += [valuepair]
    #     # print(f"set valuepairs {self.valuepairs}")

    # def setrecorddict(self, column_names):
    #     self.recorddict = {}
    #     for index, name in enumerate(column_names[1:]):
    #         self.recorddict.update({name: self.recordarray[1:][index]})
    #     # print(f"set recorddict {self.recorddict}")

    # def get_column_value(self, column_name):
    #     """
    #     method to easily retrieve a value for a specific column
    #     """

    #     # loop over all records valuepairs
    #     for valuepair in self.valuepairs:
            
    #         # if column name is found return the value
    #         if valuepair[0] == column_name:

    #             column_value = valuepair[1]
    #             return column_value

    #     print("Column not found")
    #     return "Column not found"