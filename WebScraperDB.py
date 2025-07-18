import configparser
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import engine_from_config
import pandas as pd

class WebScraperDB():
    def __init__(self, config_filename):
        self.config = configparser.RawConfigParser()
        self.config.read(config_filename)
        db_con_string = f"{self.config.get('postgresql', 'url')}://{self.config.get('postgresql', 'user')}:{self.config.get('postgresql', 'password')}@{self.config.get('postgresql', 'host')}:{self.config.get('postgresql', 'port')}"
        self.psql_engine = create_engine(db_con_string)

    def _show_table(self, table_name, return_data=False):
        query = f"""SELECT *
            FROM {table_name}
        """
        with self.psql_engine.connect() as connection:
            if return_data:
                return pd.read_sql(query, connection)
            else:
                print(pd.read_sql(query, connection))
            
    def drop_table(self, table_name, return_data=False):
        drop_query = f"""DROP TABLE {table_name}"""
        answer = ''
        while answer != 'DROP':
            try:
                choice = input(f"Are you sure you want to drop {table_name}?"
                                " Please type DROP to drop table: ")
                answer = choice
            except ValueError as e:
                    f'{e} is not a valid answer.  Please type DROP to drop table.'
        if choice == 'DROP':
            with self.psql_engine.connect() as connection:
                    connection.execute(drop_query)
            print(f'{table_name} dropped successfully.')

    def _create_table(self, show_table=False):
        table_name = self.config.get('postgresql', 'database')
        create_table_query = f'''CREATE TABLE IF NOT EXISTS {table_name} (
            job_id varchar(45) NOT NULL,
            job_title varchar (100),
            organisation varchar(100),
            salary varchar(100),
            location varchar(300),
            summary text,
            job_url varchar (100),
            updated timestamp NOT NULL DEFAULT NOW(),
            PRIMARY KEY (job_id)
        );'''

        create_index_query = f"""
            CREATE UNIQUE INDEX IF NOT EXISTS jobid_title_org
            ON {table_name} (job_id, job_title, organisation);
            """

        try:
            with self.psql_engine.connect() as connection:
                connection.execute(create_table_query)
                connection.execute(create_index_query)
                if show_table:
                    print(f"Table '{table_name.upper()}' created successfully in PostgreSQL: ")
                    self._show_table(table_name)
        except (Exception, psycopg2.DatabaseError) as error :
            print ("Error while creating PostgreSQL table", error)

    def commit_rows_to_db(self, rows_to_insert):
        self._create_table()
        try:
            con = self.psql_engine.raw_connection()
            cur = con.cursor()
            # create insert statement
            args_str = ','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s)', x).decode('utf-8') for x in rows_to_insert)
            insert_query = f"""INSERT INTO jobs_database (job_id, job_title, \
                organisation, salary, location, summary, job_url)
                VALUES {args_str}
                ON CONFLICT (job_id)
                DO NOTHING;
                """
            # execute query, close connection
            cur.execute(insert_query)
            con.commit()
            con.close()
        except Exception as error:
            print ("Oops! An exception has occured:", error)
            print ("Exception TYPE:", type(error))
            print(insert_query)