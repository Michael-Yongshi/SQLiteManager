
class Record(object):
    def __init__(self, column_names, values):
        super().__init__()

        """
        primarykey: primary key of this record
        dict: dictionary with key and values

        record.print() will print the record to the terminal and return the string printed
        """

        self.column_names = column_names
        self.values = values
        self.dict = {}
        for 
        self.primarykey = self.dict["id"]

    def print(self):

        print_string = ""
        for key, value in self.dict:
            print_string += f"{key}: {value}, "
        print_string = print_string[:-2]

        print(print_string)
        return print_string

    def get_column_names(self):

        column_names = []
        for key in self.dict:
            column_names += [key]

        return column_names

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