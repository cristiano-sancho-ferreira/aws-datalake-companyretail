# python lambda_function.py "{'database_name':'csancho_datalake_dev_raw','sql_files_directory':'.\\tabelas','athena_query_output':'s3://aws-athena-resultquery/'}"


import boto3
import sys
import os
import fnmatch
import ast
import logging
from time import time, sleep
from typing import List

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def run(sql_files: List[str],
        sql_files_directory: str,
        database_name: str,
        athena_query_output: str) -> None:
    # protect delete all when we have specific table

    for file_name in sql_files:
        cleanup_table(sql_files_directory,
                      database_name,
                      file_name,
                      athena_query_output)


def cleanup_table(sql_files_directory: str,
                  database_name: str,
                  file_name: str,
                  athena_query_output: str) -> None:
    table_name = file_name.split('.')[0]
    print(f'table_name: {table_name}')

    athena_client = boto3.client('athena')

    print("Excluir a tabela")
    sql_drop = f"DROP TABLE IF EXISTS {database_name}.{table_name}"
    print(f"sql_drop: {sql_drop}")
    athena_client.start_query_execution(QueryString=sql_drop,
                                        ResultConfiguration={'OutputLocation': athena_query_output})

    # avoid fast create and problem
    sleep(1)

    print(f"Obtendo o script para criar a tabela")
    file_tb = f"{sql_files_directory}/{table_name}.sql"
    sql_file = open(file_tb, 'r')

    print(f"Cria a tabela no Athena")
    sql_create_table = sql_file.read()
    print(f"sql_create_table: {sql_create_table}")
    sql_file.close()
    athena_client.start_query_execution(QueryString=sql_create_table,
                                        ResultConfiguration={'OutputLocation': athena_query_output})


def main():
    try:
        print(f"sys.argv {len(sys.argv)}: {sys.argv}")
        if len(sys.argv) != 2:
            raise Exception("Wrong number of arguments")

        start_time: float = time()

        program_param = ast.literal_eval(sys.argv[1])

        sql_files_directory = program_param['sql_files_directory']
        database_name = program_param['database_name']
        table = ''
        athena_query_output = program_param['athena_query_output']

        sql_files = fnmatch.filter(os.listdir(
            f'{sql_files_directory}'), '*.sql')
        if table:
            sql_files = fnmatch.filter(sql_files, f'{table}.sql')

        print(f'sql_files: {sql_files}')
        print(f'sql_files_directory: {sql_files_directory}')

        if len(sql_files) == 0:
            raise Exception("Files not found")

        run(sql_files,
            sql_files_directory,
            database_name,
            athena_query_output)

        print(f"Time elapsed: {time() - start_time} seconds")

    except Exception as ex:
        logger.error(str(ex), exc_info=True)


if __name__ == "__main__":
    main()
