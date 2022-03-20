import os
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")


class DirectoryDB:
    __config = {
        "DATABASE_LOCATION": "DATABASES"
    }

    DATABASE_NOT_FOUND: int = -1
    DATABASE_ALREADY_EXISTS: int = -2
    TABLE_NOT_FOUND: int = -3
    TABLE_ALREADY_EXISTS: int = -4
    DATABASE_TABLE_NOT_FOUND: int = -5
    INVALID_INPUT: int = -6
    COLUMN_NOT_FOUND: int = -7
    SUCCESS: int = 1
    LOG: bool = True
    WRITE_LOG: bool = False
    LOG_PATH = None

    __EMPTY: str = ""

    def __GetCurrentTime(self):
        now = datetime.now()
        return now.strftime("%Y/%m/%d %I:%M:%S %p")

    def __log(self, *args):
        if self.LOG: print(*args)
        if self.WRITE_LOG:
            with open(os.path.join(self.LOG_PATH, self.__config['DATABASE_LOCATION'] + ".log"), "a") as log:
                log_data = str(*args) + " - " + self.__GetCurrentTime() + " \n"
                log.write(log_data)
                log.close()

    def __GetTablePath(self, db: str, table: str) -> str:
        return os.path.join(self.__config['DATABASE_LOCATION'], db, table, table + ".csv")

    def __init__(self, ParentDB_Name_or_Path):
        if not os.path.isdir(ParentDB_Name_or_Path):
            os.mkdir(ParentDB_Name_or_Path)
        self.__config["DATABASE_LOCATION"] = ParentDB_Name_or_Path
        self.LOG_PATH = os.path.join(ParentDB_Name_or_Path)
        self.__log("DIRECTORY_DB INITIALIZED SUCCESSFULLY!")

    def CheckDBExists(self, db: str) -> int:
        if os.path.isdir(os.path.join(self.__config['DATABASE_LOCATION'], db)):
            return self.SUCCESS
        return self.DATABASE_NOT_FOUND

    def CheckDBTable(self, db: str, table: str) -> int:
        local_db = True
        local_tb = True
        if not self.CheckDBExists(db):
            self.__log(db + " DATABASE NOT EXISTS")
            local_db = False
        if not self.CheckTableExists(db, table):
            self.__log(table + " TABLE NOT EXISTS")
            local_tb = False
        if not local_db and not local_tb: return self.DATABASE_TABLE_NOT_FOUND
        if not local_db: return self.DATABASE_NOT_FOUND
        if not local_tb: return self.TABLE_NOT_FOUND
        return self.SUCCESS

    def CheckTableExists(self, db: str, table: str) -> int:
        if not self.CheckDBExists(db):
            self.__log("DATABASE NOT EXISTS - INVALID DB ")
            return self.DATABASE_NOT_FOUND
        if os.path.isdir(os.path.join(self.__config['DATABASE_LOCATION'], db, table)):
            return self.SUCCESS
        return self.TABLE_NOT_FOUND

    def CreateDB(self, db: str) -> int:
        if self.CheckDBExists(db) != self.SUCCESS:
            os.mkdir(os.path.join(self.__config['DATABASE_LOCATION'], db))
            self.__log(db + " DATABASE CREATED SUCCESSFULLY!")
            return self.SUCCESS
        return self.DATABASE_ALREADY_EXISTS

    def DeleteDB(self, db: str) -> int:
        if self.CheckDBExists(db) != self.SUCCESS:
            self.__log("DATABASE NOT EXISTS")
            return self.DATABASE_NOT_FOUND
        shutil.rmtree(os.path.join(self.__config['DATABASE_LOCATION'], db))
        self.__log(db + " DATABASE DELETED SUCCESSFULLY!")
        return self.SUCCESS

    def EmptyDB(self, db: str) -> int:
        if self.CheckDBExists(db) != self.SUCCESS:
            self.__log("DATABASE NOT EXISTS")
            return self.DATABASE_NOT_FOUND
        tables = os.listdir(os.path.join(self.__config['DATABASE_LOCATION'], db))
        for table in tables:
            shutil.rmtree(os.path.join(self.__config['DATABASE_LOCATION'], db, table))
        self.__log("SUCCESSFULLY DELETED ALL TABLES OF " + db)
        return self.SUCCESS

    def CreateTable(self, db: str, table: str, columns: list) -> int:
        if self.CheckDBExists(db) != self.SUCCESS:
            self.__log(db + " DATABASE NOT EXISTS")
            return self.DATABASE_NOT_FOUND
        if self.CheckTableExists(db, table) == self.SUCCESS:
            self.__log(table + " TABLE ALREADY EXISTS")
            return self.TABLE_NOT_FOUND
        if not isinstance(columns, list):
            self.__log("INVALID DATATYPE FOR COLUMN -- COLUMN SHOULD BE IN LIST")
            return self.INVALID_INPUT
        columns_dict = {}
        for l in columns:
            columns_dict[l] = []
        table_path = os.path.join(self.__config['DATABASE_LOCATION'], db, table)
        os.mkdir(table_path)
        df = pd.DataFrame(columns_dict)
        df.to_csv(self.__GetTablePath(db, table), index=False)
        self.__log(table + " TABLE CREATED SUCCESSFULLY!")
        return self.SUCCESS

    def RenameTable(self, db: str, table: str, new_table_name: str) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists

        table_path = os.path.join(self.__config['DATABASE_LOCATION'], db)
        try:
            os.rename(os.path.join(table_path, table), os.path.join(table_path, new_table_name))
            os.rename(os.path.join(table_path, new_table_name, table + ".csv"),
                      os.path.join(table_path, new_table_name, new_table_name + ".csv"))
        except:
            pass
        self.__log("TABLE RENAMED SUCCESSFULLY!")
        return self.SUCCESS

    def AddColumnToTable(self, db: str, table: str, columns: list) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists

        if not isinstance(columns, list):
            self.__log("INVALID DATATYPE FOR COLUMN -- COLUMN SHOULD BE IN LIST")
            return self.INVALID_INPUT

        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        for column in columns:
            table_file[column] = self.__EMPTY
        table_file.to_csv(table_path, index=False)
        self.__log("COLUMNS APPENDED SUCCESSFULLY!")
        return self.SUCCESS

    def RemoveColumnFromTable(self, db: str, table: str, columns: list) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists

        if not isinstance(columns, list):
            self.__log("INVALID DATATYPE FOR COLUMN -- COLUMN SHOULD BE IN LIST")
            return self.INVALID_INPUT

        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            table_file.drop(columns=columns, inplace=True)
            self.__log("COLUMNS REMOVED SUCCESSFULLY!")
        except:
            self.__log("COLUMN NOT EXISTS " + str(columns))
            return self.INVALID_INPUT
        table_file.to_csv(table_path, index=False)
        return self.SUCCESS

    def RenameColumn(self, db: str, table: str, columns: list) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists

        if not isinstance(columns, dict):
            self.__log("COLUMNS SHOULD IN DICT FORMAT")
            return self.INVALID_INPUT
        table_path = self.__GetTablePath(db, table)
        table = pd.read_csv(table_path)
        table.rename(columns=columns, inplace=True)
        table.to_csv(table_path, index=False)
        self.__log("COLUMNS RENAMED SUCCESSFULLY")
        return self.SUCCESS

    def InsertRows(self, db: str, table: str, data: list) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table = pd.read_csv(table_path)
        if not isinstance(data, list):
            self.__log("DATA SHOULD BE IN LIST FORMAT")
            return self.INVALID_INPUT
        for d in data:
            table = table.append(d, ignore_index=True)
        table.to_csv(table_path, index=False)
        self.__log("ROWS INSERTED SUCCESSFULLY")
        return self.SUCCESS

    def FetchFirstRow(self, db: str, table: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_data = pd.read_csv(table_path)
        return table_data.iloc[:1]

    def FetchAllRows(self, db: str, table: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_data = pd.read_csv(table_path)
        return table_data

    def FetchLastRow(self, db: str, table: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_data = pd.read_csv(table_path)
        return table_data.iloc[-1:]

    def FetchRowsOnCondition(self, db: str, table: str, query: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        if not isinstance(query, str):
            self.__log("CONDITIONS SHOULD BE IN LIST")
            return self.INVALID_INPUT
        table_path = self.__GetTablePath(db, table)
        table_data = pd.read_csv(table_path)
        return table_data.query(query)

    def FetchRowViaIndex(self, db: str, table: str, index: list):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            return table_file.iloc[index]
        except:
            self.__log(table + " TABLE DATA DROP OUT OF RANGE FROM " + db + " DATABASE")
        return table_file

    def FetchRowInRange(self, db: str, table: str, data_range: tuple):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        return table_file[data_range[0]:data_range[1]]

    def GetTableGraph(self, db: str, table: str, column: str, exclude: list = [], save: bool = False, path: str = None,
                      filename: str = None, show: bool = False) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists

        table_path = self.__GetTablePath(db, table)
        plt.rcParams["figure.figsize"] = [7.50, 3.50]
        plt.rcParams["figure.autolayout"] = True
        table_data = pd.read_csv(table_path)
        table_data.drop(columns=exclude, inplace=True)
        table_data.set_index(column).plot()
        plt.title(table)
        if show:
            plt.show()
        if save and path is not None and filename is not None:
            plt.savefig(os.path.join(path, filename))
        return self.SUCCESS

    def DeleteTable(self, db: str, table: str) -> int:
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        shutil.rmtree(os.path.join(self.__config['DATABASE_LOCATION'], db, table))
        self.__log(table + " TABLE DELETED SUCCESSFULLY!")
        return self.SUCCESS

    def DropRowInRange(self, db: str, table: str, data_range: tuple):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            table_file.drop(range(data_range[0], data_range[1]), inplace=True)
        except:
            table_file.drop(range(data_range[0], (len(table_file))), inplace=True)
        table_file.to_csv(table_path, index=False)
        return table_file

    def DropRowViaIndex(self, db: str, table: str, index: list):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            table_file.drop(table_file.index[index], inplace=True)
            table_file.to_csv(table_path, index=False)
        except:
            self.__log(table + " TABLE DATA DROP OUT OF RANGE FROM " + db + " DATABASE")
        return table_file

    def DropLastRow(self, db: str, table: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            table_file.drop(table_file.index[-1], inplace=True)
            table_file.to_csv(table_path, index=False)
        except:
            pass
        return table_file

    def DropFirstRow(self, db: str, table: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            table_file.drop(table_file.index[0], inplace=True)
            table_file.to_csv(table_path, index=False)
        except:
            pass
        return table_file

    def DropRowByCondition(self, db: str, table: str, condition):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            table_file.drop(table_file.query(condition).index[0], inplace=True)
            table_file.to_csv(table_path, index=False)
        except:
            pass
        return table_file

    def UpdateLastRow(self, db: str, table: str, update_data: dict):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            for key, value in update_data.items():
                table_file.iloc[-1, table_file.columns.get_loc(key)] = value
            table_file.to_csv(table_path, index=False)
            return self.SUCCESS
        except:
            return self.COLUMN_NOT_FOUND

    def UpdateRowsInRange(self, db: str, table: str, index_range: tuple, update_data: dict):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            for key, value in update_data.items():
                table_file.loc[index_range[0]:index_range[1], key] = value
            table_file.to_csv(table_path, index=False)
            return self.SUCCESS
        except Exception as e:
            return self.COLUMN_NOT_FOUND

    def UpdateRowsByIndex(self, db: str, table: str, indexes: list, update_data: dict):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        table_file = pd.read_csv(table_path)
        try:
            last_index = table_file[table_file.columns[0]].count()
            for index in indexes:
                if index <= last_index:
                    for key, value in update_data.items():
                        table_file.loc[index, key] = value
            table_file.to_csv(table_path, index=False)
            return self.SUCCESS
        except Exception as e:
            self.__log(e)
            return self.COLUMN_NOT_FOUND

    def UpdateRowsOnCondition(self, db: str, table: str):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        try:
            table_path = self.__GetTablePath(db, table)
            table_file = pd.read_csv(table_path)
            return table_file
        except:
            return self.TABLE_NOT_FOUND

    def SaveConditionedUpdatedData(self,db: str, table: str, data):
        is_dbtb_exists = self.CheckDBTable(db, table)
        if is_dbtb_exists != self.SUCCESS: return is_dbtb_exists
        table_path = self.__GetTablePath(db, table)
        data.to_csv(table_path, index=False)
