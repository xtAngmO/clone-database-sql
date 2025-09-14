import json
from typing import Self

import mysql.connector
import mysql.connector.abstracts
import numpy as np
import pandas as pd


class Database:
    def __init__(self, host: str, user: str, password: str, port: int, db_name: str, charset: str | None = None, collation: str | None = None):
        self.connection: (mysql.connector.pooling.PooledMySQLConnection | mysql.connector.pooling.MySQLConnectionAbstract) | None = None
        self.cursor: mysql.connector.abstracts.MySQLCursorAbstract | None = None
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.charset = charset
        self.collation = collation

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                charset=self.charset,
                collation=self.collation,
            )
            if self.connection.is_connected():
                print("Database connection successful")
                return True
            else:
                print("Failed to connect to database")
                return False

        except mysql.connector.Error as err:
            print(f"MySQL Error: {err}")
            return False

        except Exception as err:
            print(f"Unexpected error: {err}")
            import traceback

            traceback.print_exc()
            return False

    def check_connection(self) -> bool:
        if not self.connection or not self.connection.is_connected():
            print("Database not connected")
            return False
        else:
            return True

    def create_table(self, operation: str):
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        self.cursor.execute(f"USE {self.db_name}")

        self.cursor.execute(operation)

        self.connection.commit()
        self.cursor.close()
        return

    def insert_data(self, data_df: pd.DataFrame, table_name: str, columns: list = None) -> bool:
        if not self.check_connection():
            return False

        cursor = self.connection.cursor()
        cursor.execute(f"USE {self.db_name}")

        data_df = data_df.replace({np.nan: None})

        if columns is None:
            columns = data_df.columns.tolist()

        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        records = []
        for _, row in data_df.iterrows():
            record = tuple(row[column] for column in columns)
            records.append(record)

        try:
            cursor.executemany(insert_query, records)
            self.connection.commit()
            print(f"Successfully inserted {cursor.rowcount} records into {table_name}")
            return True
        except mysql.connector.Error as err:
            print(f"Error inserting data into {table_name}: {err}")
            print(f"Detailed error: {err.msg}")
            return False
        finally:
            cursor.close()

    def clone_from_another_database(self, source_db: "Database") -> bool:
        if not self.check_connection() or not source_db.check_connection():
            return False

        try:
            source_cursor = source_db.connection.cursor(dictionary=True)
            target_cursor = self.connection.cursor()

            target_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.db_name}`")
            target_cursor.execute(f"USE `{self.db_name}`")
            target_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            source_cursor.execute(f"USE `{source_db.db_name}`")
            source_cursor.execute("SHOW TABLES")
            tables = source_cursor.fetchall()

            for table_row in tables:
                table_name = list(table_row.values())[0]
                print(f"Cloning table: {table_name}")

                source_cursor.execute(f"SHOW CREATE TABLE {table_name}")
                create_table_stmt = source_cursor.fetchone()["Create Table"]
                modified_create_stmt = create_table_stmt.replace(f"`{source_db.db_name}`", f"`{self.db_name}`")
                target_cursor.execute(f"USE `{self.db_name}`")
                target_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                target_cursor.execute(modified_create_stmt)
                source_cursor.execute(f"SELECT * FROM {table_name}")
                rows = source_cursor.fetchall()

                if rows:
                    columns = list(rows[0].keys())
                    columns_str = ", ".join([f"`{col}`" for col in columns])
                    placeholders = ", ".join(["%s"] * len(columns))

                    values = []
                    for row in rows:
                        row_values = [row[col] for col in columns]
                        values.append(tuple(row_values))

                    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    target_cursor.executemany(insert_query, values)
                    print(f"Inserted {len(values)} rows into {self.db_name}.{table_name}")

                self.connection.commit()

            target_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            print(f"Database {source_db.db_name} successfully cloned to {self.db_name}")
            return True

        except mysql.connector.Error as err:
            print(f"Error cloning database: {err}")
            print(f"Detailed error: {err.msg if hasattr(err, 'msg') else str(err)}")
            return False
        finally:
            try:
                if self.connection and self.connection.is_connected():
                    target_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            except:
                pass

            if "source_cursor" in locals():
                source_cursor.close()
            if "target_cursor" in locals():
                target_cursor.close()

    def clone_single_table(self, source_db: "Database", table_name: str) -> bool:
        if not self.check_connection() or not source_db.check_connection():
            return False

        try:
            source_cursor = source_db.connection.cursor(dictionary=True)
            target_cursor = self.connection.cursor()

            target_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")

            target_cursor.execute(f"USE {self.db_name}")
            target_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            source_cursor.execute(f"USE {source_db.db_name}")
            source_cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if not source_cursor.fetchone():
                print(f"Table {table_name} does not exist in {source_db.db_name}")
                return False

            print(f"Cloning table: {table_name}")

            source_cursor.execute(f"SHOW CREATE TABLE {table_name}")
            create_table_stmt = source_cursor.fetchone()["Create Table"]

            modified_create_stmt = create_table_stmt.replace(f"`{source_db.db_name}`", f"`{self.db_name}`")

            target_cursor.execute(f"USE {self.db_name}")

            target_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            target_cursor.execute(modified_create_stmt)

            source_cursor.execute(f"SELECT * FROM {table_name}")
            rows = source_cursor.fetchall()

            if rows:
                columns = list(rows[0].keys())
                columns_str = ", ".join([f"`{col}`" for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))

                values = []
                for row in rows:
                    row_values = [row[col] for col in columns]
                    values.append(tuple(row_values))

                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                target_cursor.executemany(insert_query, values)

                print(f"Inserted {len(values)} rows into {self.db_name}.{table_name}")

            self.connection.commit()

            target_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            print(f"Table {table_name} successfully cloned from {source_db.db_name} to {self.db_name}")
            return True

        except mysql.connector.Error as err:
            print(f"Error cloning table: {err}")
            print(f"Detailed error: {err.msg if hasattr(err, 'msg') else str(err)}")
            return False
        finally:
            try:
                if self.connection and self.connection.is_connected():
                    target_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            finally:
                pass

            if "source_cursor" in locals():
                source_cursor.close()
            if "target_cursor" in locals():
                target_cursor.close()

    def restore_from_sql_file(self, db_connection: Self, sql_file_path):
        try:
            with open(sql_file_path, "r", encoding="utf-8") as file:
                sql_commands = file.read()

            commands = sql_commands.split(";")

            cursor = db_connection.connection.cursor()

            for command in commands:
                if command.strip():
                    cursor.execute(command)

            db_connection.connection.commit()
            cursor.close()

            print(f"Successfully restored database from {sql_file_path}")
            return True

        except Exception as e:
            print(f"Error restoring database: {e}")
            return False

    def import_json_to_database(self, json_file_path: str, table_name: str):
        try:
            with open(json_file_path, "r", encoding="utf-8") as file:
                json_data = json.load(file)

            if "table" not in json_data or "rows" not in json_data:
                print("Invalid JSON structure. Expected 'table' and 'rows' fields.")
                return False

            if json_data["table"] != table_name:
                print(f"Warning: JSON table name '{json_data['table']}' differs from specified table name '{table_name}'")

            rows = json_data["rows"]
            if not rows:
                print("No data to import.")
                return False

            df = pd.DataFrame(rows)

            columns = df.columns.tolist()
            return self.insert_data(df, table_name, columns)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return False

        except Exception as e:
            print(f"Error importing JSON: {e}")
            return False

    def get_table(self, table_name: str, query: str = None) -> pd.DataFrame:
        if not self.check_connection():
            return pd.DataFrame()

        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(f"USE {self.db_name}")

            if query is None:
                sql_query = f"SELECT * FROM {table_name}"
            else:
                sql_query = query

            cursor.execute(sql_query)
            rows = cursor.fetchall()

            if not rows:
                print(f"No data found in table {table_name}")
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            print(f"Successfully retrieved {len(df)} rows from {table_name}")
            return df

        except mysql.connector.Error as err:
            print(f"Error retrieving data from {table_name}: {err}")
            print(f"Detailed error: {err.msg if hasattr(err, 'msg') else str(err)}")
            return pd.DataFrame()
        except Exception as err:
            print(f"Unexpected error: {err}")
            return pd.DataFrame()
        finally:
            cursor.close()