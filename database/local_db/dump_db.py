import os
import subprocess


def dump_database():
    # PostgreSQL connection details
    pg_user = os.getenv('POSTGRES_USER')
    pg_password = os.getenv('POSTGRES_PASSWORD')
    pg_host = os.getenv('POSTGRES_HOST')
    pg_port = os.getenv('POSTGRES_PORT')
    pg_db = os.getenv('POSTGRES_DATABASE')

    # Dump file name
    dump_file = 'database_dump.sql'

    # Construct pg_dump command
    dump_command = [
        'pg_dump',
        f'-h {pg_host}',
        f'-p {pg_port}',
        f'-U {pg_user}',
        f'-d {pg_db}',
        f'-f {dump_file}'
    ]

    # Execute pg_dump command
    try:
        subprocess.run(dump_command, check=True)
        print("Database dumped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error dumping database: {e}")


if __name__ == "__main__":
    dump_database()
