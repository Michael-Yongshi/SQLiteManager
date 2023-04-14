import logging

import os
from shutil import copyfile

import sqlite3
from sqlite3 import Error

class Database(object):
    def __init__(self, path="", filename="", extension="", load_all=False):

        self.complete_path, self.path, self.filename, self.extension = self.determine_complete_path(path, filename, extension)

        logging.info(f"Path is set to {self.path}")
        logging.info(f"Filename is set to {self.filename}")
        logging.info(f"File extension set to {self.extension}")

        if os.path.isfile(self.complete_path):
            logging.info(f"Opening Database")
        else:
            logging.info(f"Creating Database")

        try:
            self.connection = sqlite3.connect(self.complete_path)
            logging.info("Connection to SQLite DB successful")

        except Error as e:

            logging.warning(f"The error '{e}' occurred")

        # pull all sql tables and records
        if load_all == True:
            self.load_tables()

    def determine_complete_path(self, path_given, filename_given, extension_given):
        """
        Show all files in a folder
        """

        # if path is not given, get location of the working directory (os.getcwd method)
        if path_given != "":
            path = path_given
            if not os.path.exists(path):
            # check if directory already exists, if not create it
                os.makedirs(path)
                print(f"couldnt find path, creating directory: {path}")
        else:
            path = os.getcwd()

        # if extension is not given, get default .sqlite3
        extension = extension_given if extension_given != "" else ".sqlite3"

        # if filename is not given, check if the working directory has only one file with the extension above
        if filename_given != "":
            filename = filename_given

        else:
            filelist_exact = []
            filelist_semi = []

            # Iterate over sqlite files
            files = os.listdir(path)
            for file in files:
                name = os.path.splitext(file)[0]

                if file.endswith(".sqlite3"): 
                    filelist_exact.append(name)

                elif file.endswith(".sqlite*"):
                    filelist_semi.append(name)

            # determine if there is a single match, first exact
            if len(filelist_exact) == 1:
                filename = filelist_exact[0]

            elif len(filelist_exact) > 1:
                print(f"Couldnt determine file, found multiple exact matches {filelist_exact}")
                return

            else:
                # check for other sqlite extensions
                if len(filelist_semi) == 1:
                    filename = filelist_semi[0]

                elif len(filelist_semi) > 1:
                    print(f"Couldnt determine file, found multiple matches {filelist_exact}")

                else:
                    print(f"Couldnt determine file, found no matches in {files}")
                    return

        # build complete path
        complete_path = os.path.join(path, filename + extension)

        return complete_path, path, filename, extension

    def close(self):
        """
        Closes connection to the database if connected to one
        """

        if self.connection != None:
            self.connection.close()
            self.connection = None
        else:
            print(f"Couldn't close database, not connected to one!")

    def saveas(self, filename, path=""):
        """
        Saves the database to the new location if it doesnt exists yet
        It returns a database object that can be set to self.database if you want to continue working in the new file

        It will cancel if target file already exists, to overwrite delete the existing file
        """

        path = path if path != "" else self.path

        src = self.complete_path
        dst = os.path.join(path, filename + self.extension)

        if os.path.isfile(src):

            if os.path.exists(path) == False:
                os.makedirs(path)

            if os.path.isfile(dst):
                logging.warning(f"error destination: {dst} already exists")

            else:
                logging.info(f"copying file: {src} to {dst}")
                copyfile(src, dst)

        else:
            print(f"Source file does not exist: {src}")

    def delete(self):
        """
        Deletes database and removes pointer to it if connected to one.
        """

        if self.database != None:

            self.connection.close()
            self.connection = None

            os.remove(self.get_complete_path())

            logging.info(f"Database deleted!")

        else:
            logging.warning(f"Couldn't delete database, not connected to one!")

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
        for each ('_' denotes an unused variable) in parameters, join a string ('?') with a comma and a space (', ')
        this leaves question marks seperated by a comma space in between that SQL will expect

        then merge with query
        -query= 'SELECT name FROM students WHERE id IN (%s)' % placeholders

        meaning
        this replaces the '%s' with our placeholders ('?, ?, ?' in our case)
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