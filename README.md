# SQLiteManager
Use a convenient SQLiteHandler object to manipulate SQLite(3) databases.

## Version

### 0.7
0.7.2
- added get_latest_record function

0.7.1
- added Record.values (list of values)

0.7.0
- Completely redone, not backwards compatible at all
- Contains most of the previous functionality

### 0.6
0.6.1
- extra logging for printing path

0.6.0
- Extension type can be given to the handler to discern between .sqlite and .sqlite3
- changed many functions to depend on the location variables of the handler class (path, filename, extension)
make sure the extension variable does not miss the dot!
- removed redundent steps or functions, mostly from the database class and added to the handler
- added some more comments for functions
- other fixes

### 0.5.3
- Removed some print statements that clutter the terminal during debugging
- removed some redundent import statements

### 0.5.0
- Added test_gui for testing the library

### 0.3.x
- Adds handler.py with a SQLiteHandler object
- moves support methods to helpers.py

### 0.2.x
- Basic objects in database.py for Database, Tables and Records

## Install
```
pip install sqlitemanager
```

## How to
Primarily use the handler to do database manipulation for you instead of directly edit the data objects. The handler is built to make manipulation of the objects even simpler. The Database object contains the actual connection to the database.

See the [example.py](example.py) file for a list of examples of functionality.
https://github.com/Michael-Yongshi/SQLiteManager/blob/master/unit_tests.py

The sqlite handler is aware of the working directory, if paths are not given it will work from the current working directory.
It prints the paths its using, so watch closely that its called from the correct one.

## Tests
Run test_handler.py in order to test the package.

# Build

## Pypirc file
Create a .pypirc file in home directory

```
[distutils]
index-servers=
    pypi
    test

[test]
repository = https://test.pypi.org/legacy/
username = __token__
password = <PyPI token>

[pypi]
repository = https://upload.pypi.org/legacy/
username = __token__
password = <PyPI token>
```

## Distributions

```
python3 -m pip install --upgrade build && python3 -m build
```

## Upload

```
python3 -m pip install --upgrade twine && python3 -m twine upload --repository pypi dist/*
```

## Licence

Licensed under GPL-3.0-or-later, see LICENSE file for details.

Copyright © 2020 Michael-Yongshi.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.
