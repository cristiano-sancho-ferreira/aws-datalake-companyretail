# encoding: utf-8
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
import sys
import boto3
from datetime import datetime
import time
import pg8000 as dbapi
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'interfacegroup', 'country', 'business', 'environment', 'schema_cdc',
                                     'interface_type', 'access_role_write_redshift', 'access_role_read_redshift'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
environment_region = glueContext._jvm.AWSConnectionUtils.getRegion()

sc._jsc.hadoopConfiguration().set('fs.s3.enableServerSideEncryption', 'true')
sc._jsc.hadoopConfiguration().set('fs.s3.serverSideEncryptionAlgorithm', 'AES256')

spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.speculation", "false")

s3 = boto3.resource('s3')
client = boto3.client('glue', region_name=environment_region)

jdbc_url = ''
jdbc_user = ''
jdbc_password = ''
jdbc_url_sandbox = ''
jdbc_user_sandbox = ''
jdbc_password_sandbox = ''
jdbc_driver = "com.databricks.spark.redshift"
schema_cdc = args['schema_cdc']
access_role_read_redshift = args['access_role_read_redshift']
access_role_write_redshift = args['access_role_write_redshift']

group_interface = ''

errors = []
executed_tables = []
redshift_connection = None
tmp_bucket = "cencosud.{}.ldm.{}.{}".format(args['environment'], args['business'], args['country'])
tmp_directory = "s3://{}/redshift/{}".format(tmp_bucket, args['interfacegroup'])


def main():
    connect_redshift()
    list_tables_process = read_cdc()
    global group_interface

    for table in list_tables_process:

        group_interface = table.interface_group
        load_table(table)

    close_connection()
    end_execution()


def end_execution():
    if errors:
        message = ''
        for error in errors:
            message += '####ERROR#### while executing the load process in table {}, because {} \n\n' \
                .format(error['TableName'], error['Message'])

        raise Exception("###ERROR#### Error while executing job, \n {}".format(message))
    else:
        print("#### Job successfully executed ####")


def close_connection():
    try:
        if redshift_connection:
            redshift_connection.close()
    except Exception as e:
        print("###ERROR#### on close connection with redshift, because {}".format(e))


def add_control_table(interface_name, return_code, message):
    truncated_message = message
    if len(message) >= 1000:
        truncated_message = message[:900]

    truncated_message = truncated_message.replace("'", "")

    table = '{}.p_control'.format(schema_cdc)

    query = "INSERT INTO {} (interface_group,interface_name,date_process,return_code,return_dsc) VALUES " \
            "('{}', '{}', '{}', {}, '{}')".format(table, group_interface, interface_name,
                                                datetime.today().strftime('%Y-%m-%d %H:%M:%S'), return_code,
                                                truncated_message)

    cursor = redshift_connection.cursor()
    cursor.execute(query)
    cursor.close()


def add_error(table_name, message):
    errors.append({
        'TableName': table_name,
        'Message': message
    })

    print(message)


def update_control_table(interface_name, status, partition_dsc):

    query = "UPDATE {}.p_cdc SET proc_status = '{}', proc_date = '{}' where table_status = 'processed' AND proc_status IS NULL AND interface_group = '{}' AND interface_name = '{}' AND partition_dsc = '{}'".format(schema_cdc, status, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), group_interface, interface_name, partition_dsc.replace("'", "''"))

    cursor = redshift_connection.cursor()
    cursor.execute(query)
    cursor.close()


def load_table(table):
    interface_name = table.interface_name.lower()
    partition_dsc = table.partition_dsc.lower()
    table_name = interface_name[interface_name.find(".") + 1:]

    try:

        if partition_dsc == '-1':

            sql_query = "select * from {}".format(interface_name)

            df = spark.read.format(jdbc_driver) \
                .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
                .option("query", sql_query) \
                .option("aws_iam_role", access_role_read_redshift) \
                .option("tempdir", tmp_directory) \
                .load()

            time.sleep(5)

            df.write \
                .format(jdbc_driver) \
                .option("url", jdbc_url_sandbox + "?user=" + jdbc_user_sandbox + "&password=" + jdbc_password_sandbox) \
                .option("dbtable", interface_name) \
                .option("aws_iam_role", access_role_write_redshift) \
                .option("tempdir", tmp_directory) \
                .mode("overwrite") \
                .save()
                
            add_control_table(interface_name, 0, "Table {} successfully loaded in the Redshift".format(table_name))

        else:

            preactions_sql_query = "delete from {} where {}".format(interface_name, partition_dsc)

            sql_query = "select * from {} where {}".format(interface_name, partition_dsc)

            df = spark.read.format(jdbc_driver) \
                .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
                .option("query", sql_query) \
                .option("aws_iam_role", access_role_read_redshift) \
                .option("tempdir", tmp_directory) \
                .load()

            time.sleep(5)

            df.write \
                .format(jdbc_driver) \
                .option("url", jdbc_url_sandbox + "?user=" + jdbc_user_sandbox + "&password=" + jdbc_password_sandbox) \
                .option("preactions", preactions_sql_query) \
                .option("dbtable", interface_name) \
                .option("aws_iam_role", access_role_write_redshift) \
                .option("tempdir", tmp_directory) \
                .mode("append") \
                .save()

            add_control_table(interface_name, 0, "Partition {} of Table {} successfully loaded in the Redshift".format(partition_dsc, table_name))

        update_control_table(table.interface_name, 'processed', table.partition_dsc)

    except Exception as e:
        message = "####ERROR#### in load_table; because: {}".format(e)
        add_error(table_name, message)


def read_cdc():
    try:
        interface_group = args['interfacegroup']
        interface_type = args['interface_type']

        if interface_group == 'all':
            clause_interface_group = "like 'sandbox_%'"
        else:
            clause_interface_group = "= '{}'".format(interface_group)

        if interface_type == 'all':
            clause_interface_type = ""
        else:
            clause_interface_type = "AND interface_type = '{}'".format(interface_type)

        sql_query = "SELECT DISTINCT interface_group, interface_name, partition_dsc FROM {}.p_cdc WHERE table_status = 'processed' AND proc_status is null AND interface_group {} {}".format(schema_cdc, clause_interface_group, clause_interface_type)

        df = spark.read.format(jdbc_driver) \
            .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
            .option("query", sql_query) \
            .option("aws_iam_role", access_role_read_redshift) \
            .option("tempdir", tmp_directory) \
            .load()

        return df.head(df.count())
    except Exception as e:
        print("####ERROR#### in read_cdc; because: {}".format(e))


def connect_redshift():
    host = jdbc_url[jdbc_url.index('//') + 2: jdbc_url.find(':', jdbc_url.index('//') + 2)]
    port = jdbc_url[jdbc_url.find(':', jdbc_url.index('//') + 2) + 1: jdbc_url.find('/', jdbc_url.find(':',
                                                                                                       jdbc_url.index(
                                                                                                           '//') + 2))]
    database_name = jdbc_url[jdbc_url.find('/', jdbc_url.find(':', jdbc_url.index('//') + 2) + 1) + 1:]

    global redshift_connection

    redshift_connection = dbapi.connect(user=jdbc_user, host=host, port=int(port), database=database_name,
                                        password=jdbc_password)

    redshift_connection.autocommit = True


def get_job_definitions():
    global jdbc_url_sandbox, jdbc_user_sandbox, jdbc_password_sandbox, jdbc_url, jdbc_user, jdbc_password

    try:
        job_conf = client.get_job(JobName=args['JOB_NAME'])
        glue_conn_name = str(job_conf.get("Job", "none").get("Connections", "none").get("Connections", "none")[0])
        glue_conn_read_cluster = client.get_connection(Name=glue_conn_name)
        glue_conn_name = str(job_conf.get("Job", "none").get("Connections", "none").get("Connections", "none")[1])
        glue_conn_write_cluster = client.get_connection(Name=glue_conn_name)

        jdbc_url = glue_conn_read_cluster['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
        jdbc_user = glue_conn_read_cluster['Connection']['ConnectionProperties']['USERNAME']
        jdbc_password = glue_conn_read_cluster['Connection']['ConnectionProperties']['PASSWORD']

        jdbc_url_sandbox = glue_conn_write_cluster['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
        jdbc_user_sandbox = glue_conn_write_cluster['Connection']['ConnectionProperties']['USERNAME']
        jdbc_password_sandbox = glue_conn_write_cluster['Connection']['ConnectionProperties']['PASSWORD']

    except Exception as e:
        raise Exception("Job Was Unsuccessful; in get_job_definitions #### because {}".format(e))


get_job_definitions()

main()
