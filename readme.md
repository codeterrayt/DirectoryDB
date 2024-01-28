# 📁 DirectoryDB

DirectoryDB is a lightweight Python-based database system designed to simplify database operations within Python projects. It offers a comprehensive set of functions covering various aspects of database management. This project provides a convenient way to import and seamlessly integrate a database system without relying on complex queries.

## Project Overview

DirectoryDB is constructed from the ground up, leveraging file system CSV This database system is built from scratch, and utilizing essential Python libraries such as Pandas, os, shutil, matplotlib, and datetime. The project strictly adheres to Python conventions, ensuring type-defined variables, functions, and return types. This adherence enhances readability and maintainability, making it a robust choice for Python developers.

## Features

### Database Operations
- `CreateDB`: Create a new database.
- `DeleteDB`: Delete an existing database.
- `EmptyDB`: Empty the contents of a database.

### Table Operations
- `CreateTable`: Create a new table within a database.
- `AddColumnToTable`: Add a new column to an existing table.
- `RenameColumn`: Rename a column in a table.
- `RemoveColumnFromTable`: Remove a column from a table.
- `RenameTable`: Rename an existing table.
- `DeleteTable`: Delete an existing table.

### Check Exists
- `CheckTableExists`: Check if a table exists in a database.
- `CheckDBExists`: Check if a database exists.
- `CheckDBTable`: Check if a database and table combination exists.

### Insert Rows
- `InsertRows`: Insert new rows into a table.

### Fetch Rows
- `FetchFirstRow`: Retrieve the first row from a table.
- `FetchLastRow`: Retrieve the last row from a table.
- `FetchAllRows`: Retrieve all rows from a table.
- `FetchRowsOnCondition`: Retrieve rows based on a specified condition.
- `FetchRowViaIndex`: Retrieve a row by its index.
- `FetchRowInRange`: Retrieve rows within a specified range.

### Update Rows
- `UpdateLastRow`: Update the last row in a table.
- `UpdateRowsInRange`: Update rows within a specified range.
- `UpdateRowsByIndex`: Update rows based on their index.
- `UpdateRowsOnCondition`: Update rows based on a specified condition.
- `SaveConditionedUpdatedData`: Save the updated data after applying a condition.

### Drop/Delete Rows
- `DropRowInRange`: Drop rows within a specified range.
- `DropRowViaIndex`: Drop a row based on its index.
- `DropLastRow`: Drop the last row from a table.
- `DropFirstRow`: Drop the first row from a table.
- `DropRowByCondition`: Drop rows based on a specified condition.

### Show Data in Graph
- `GetTableGraph`: Generate a graph for visualizing the data in a table.

## Installation

To use DirectoryDB in your Python project, clone the GitHub repository:

```bash
git clone https://github.com/codeterrayt/DirectoryDB.git
