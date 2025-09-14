from database import Database
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    database_source = Database(
        host=os.getenv("SOURCE_DB_HOST"),
        user=os.getenv("SOURCE_DB_USER"),
        password=os.getenv("SOURCE_DB_PASSWORD"),
        port=os.getenv("SOURCE_DB_PORT"),
        db_name=os.getenv("SOURCE_DB_DATABASE"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )
    database_source.connect()

    database_target = Database(
        host=os.getenv("TARGET_DB_HOST"),
        user=os.getenv("TARGET_DB_USER"),
        password=os.getenv("TARGET_DB_PASSWORD"),
        port=os.getenv("TARGET_DB_PORT"),
        db_name=os.getenv("TARGET_DB_DATABASE"),
        charset="utf8mb4",
        collation="utf8mb4_unicode_ci",
    )
    database_target.connect()
    database_target.clone_from_another_database(database_source)
    print("clone finish")
