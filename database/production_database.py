import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from database.database_interface import (
    DatabaseInterface,
    DatabaseConnectionError,
)
from database.production_db.database import engine, SessionLocal

load_dotenv()


class ProductionDatabase(DatabaseInterface):
    def __init__(self):
        """
        Initialise a connection to the production database, allows access to
        """
        try:
            self.engine = engine
            self.session = SessionLocal()
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(
                f"Error occurred while creating the database engine: {e}"
            )

    def connect(self) -> None:
        try:
            self.session = sessionmaker(bind=self.engine)()
        except SQLAlchemyError as e:
            print(f"Error occurred during database connection: {e}")
            raise DatabaseConnectionError(
                f"Error occurred during database connection: {e}"
            )

    def read_table(self, table_name: str) -> pd.DataFrame:
        try:
            table_data = pd.read_sql_table(table_name, self.engine)
            return table_data
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(
                f"Error occurred while reading table {table_name}: {e}"
            )

    def write_to_table(self, data: pd.DataFrame, table_name: str) -> pd.DataFrame:
        try:
            data.to_sql(table_name, self.engine, if_exists="replace", index=False)
            return self.read_table(table_name)
        except SQLAlchemyError as e:
            raise DatabaseConnectionError(
                f"Error occurred while writing to table {table_name}: {e}"
            )

    def table_exists(self, table_name: str) -> bool:
        # Check if the table exists in the database
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def append_rows(self, data: pd.DataFrame, table_name: str) -> bool:
        try:
            # Directly attempt to append data to the table
            data.to_sql(table_name, self.engine, if_exists="append", index=False)
            return True
        except SQLAlchemyError as e:
            # Catch any SQLAlchemy errors that occur during the append operation
            raise DatabaseConnectionError(
                f"Error occurred while appending data to table {table_name}: {e}"
            )

    def upsert_rows(self, data: pd.DataFrame, table_name: str, primary_key: str) -> bool:
        try:
            # Start a transaction
            self.session.begin()

            primary_keys = data[
                primary_key
            ].unique()  # Get unique primary keys from DataFrame

            # Delete existing records in bulk
            delete_stmt = f"DELETE FROM {table_name} WHERE {primary_key} IN :primary_keys"
            self.session.execute(text(delete_stmt), {"primary_keys": tuple(primary_keys)})

            self.session.commit()

            # Append new rows
            success = self.append_rows(data, table_name)
            if not success:
                raise Exception("Failed to append rows")

            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            raise DatabaseConnectionError(
                f"Error occurred while upserting data to table {table_name}: {e}"
            )
