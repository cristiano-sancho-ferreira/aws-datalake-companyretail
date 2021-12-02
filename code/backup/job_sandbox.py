# encoding: utf-8
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
import sys
import boto3
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'interfacegroup', 'country', 'business', 'environment', 'schema_cdc', 'access_role_read_redshift',
                                     'interface_type'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
environment_region = glueContext._jvm.AWSConnectionUtils.getRegion()

sc._jsc.hadoopConfiguration().set('fs.s3.enableServerSideEncryption', 'true')
sc._jsc.hadoopConfiguration().set('fs.s3.serverSideEncryptionAlgorithm', 'AES256')

s3 = boto3.resource('s3')
client = boto3.client('glue', region_name=environment_region)

jdbc_url = ''
jdbc_user = ''
jdbc_password = ''
jdbc_driver = "com.databricks.spark.redshift"
schema_cdc = args['schema_cdc']
access_role_read_redshift = args['access_role_read_redshift']

bucket_sand_box = ''
database_sandbox = ''
group_interface = ''

errors = []
executed_tables = []
redshift_connection = None
tmp_bucket = "cencosud.{}.ldm.{}.{}".format(args['environment'], args['business'], args['country'])
tmp_directory = "s3://{}/sandbox/{}".format(tmp_bucket, args['interfacegroup'])


def generate_nomenclature(interface_type):
    global database_sandbox, bucket_sand_box

    if interface_type == 'dm':
        type_value = "datamart"
    else:
        type_value = interface_type

    bucket_sand_box = "cencosud.{}.{}.{}.{}".format(args['environment'], type_value, args['business'],
                                                    args['country'])
    database_sandbox = "cencosud_{}_{}_{}_{}".format(args['environment'], type_value, args['business'],
                                                     args['country'])


def main():
    connect_redshift()
    list_tables_process = read_cdc()
    global group_interface

    for table in list_tables_process:
        
        group_interface = table.interface_group.lower()
        generate_nomenclature(table.interface_type.lower())
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


def add_error(table_name, message):
    errors.append({
        'TableName': table_name,
        'Message': message
    })

    print(message)


def exists_catalog(table_name):
    try:
        result = client.get_table(DatabaseName=database_sandbox, Name=table_name)

        if 'Table' in result:
            return True
        else:
            return False
    except Exception:
        return False


def add_catalog_partitioned(table_name, df, partition_column, location, interface_name):
    fields_with_partitioned = list(filter(lambda field: field[0] == partition_column, df.dtypes))
    fields_without_partitioned = list(filter(lambda field: field[0] != partition_column, df.dtypes))

    if fields_with_partitioned:
        client.create_table(
            DatabaseName=database_sandbox,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': list(map(lambda field: {'Name': field[0], 'Type': field[1]}, fields_without_partitioned)),
                    'Location': location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    }
                },
                'PartitionKeys': list(map(lambda field: {'Name': field[0], 'Type': field[1]}, fields_with_partitioned)),
                'TableType': "EXTERNAL_TABLE"
            }
        )

    else:
        message = "####ERROR#### in add_catalog_partitioned; because: the partition column '{}' does not exist in " \
                  "table {}".format(partition_column, interface_name)
        add_control_table(interface_name, 9, message)
        add_error(table_name, message)


def add_catalog(table_name, df, location):
    client.create_table(
        DatabaseName=database_sandbox,
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': list(map(lambda field: {'Name': field[0], 'Type': field[1]}, df.dtypes)),
                'Location': location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters': {
                        'serialization.format': '1'
                    }
                }
            },
            'TableType': "EXTERNAL_TABLE"
        })


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


def update_control_table(interface_name, status, table_reprocess, partition_dsc):
    query = "UPDATE {}.p_cdc SET table_status = '{}', is_reprocessing = '{}', date_process = '{}' where table_status = 'unprocessed' AND interface_group = '{}' AND interface_name = '{}' AND partition_dsc = '{}'".format(schema_cdc, status, table_reprocess, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), group_interface, interface_name, partition_dsc.replace("'", "''"))

    cursor = redshift_connection.cursor()
    cursor.execute(query)
    cursor.close()


def load_table(table):
    interface_name = table.interface_name.lower()
    partition_dsc = table.partition_dsc.lower()
    table_name = interface_name[interface_name.find(".") + 1:]

    try:
        bucket = "s3://{}/{}".format(bucket_sand_box, table_name)
        table_reprocess = 'n'

        if partition_dsc == '-1':

            sql_query = "select * from {}".format(interface_name)

            df = spark.read.format(jdbc_driver) \
                .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
                .option("query", sql_query) \
                .option("aws_iam_role", access_role_read_redshift) \
                .option("tempdir", tmp_directory) \
                .load()

            if not exists_catalog(table_name):
                add_catalog(table_name, df, bucket)

            df.write.mode("overwrite").parquet(bucket)

            add_control_table(interface_name, 0, "Table {} successfully loaded in the Sandbox".format(table_name))

        else:
            partition = split_partition(partition_dsc)

            sql_query = "select * from {} where {}".format(interface_name, partition_dsc)

            df = spark.read.format(jdbc_driver) \
                .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
                .option("query", sql_query) \
                .option("aws_iam_role", access_role_read_redshift) \
                .option("tempdir", tmp_directory) \
                .load()

            if not exists_catalog(table_name):
                add_catalog_partitioned(table_name, df, partition[0], bucket, interface_name)

            query_result = get_partitions_queries(partition_dsc, table_name)

            table_reprocess = query_result['Reprocess']

            df.write.mode("append").partitionBy(partition[0]).parquet(bucket)

            create_partition(query_result['Query'])

            add_control_table(interface_name, 0, "Partition {} of Table {} successfully loaded in the Sandbox".format(partition_dsc, table_name))
            
        update_control_table(table.interface_name, 'processed', table_reprocess, table.partition_dsc)

    except Exception as e:
        message = "####ERROR#### in load_table; because: {}".format(e)
        add_control_table(interface_name, 9, message)
        add_error(table_name, message)


def split_partition(partition):
    list_return = []
    value = partition.replace("'", "")
    list_return.append(value[:value.find("=")])
    list_return.append(value[value.find("=") + 1:])

    return list_return


def get_partitions_queries(partition_dsc, table_name):
    try:
        table_reprocess = 'n'

        for file in s3.Bucket(bucket_sand_box).objects.filter(
                Prefix="{}/{}/".format(table_name, partition_dsc.replace("\'", ""))):
            if file.key.endswith('.parquet'):
                table_reprocess = 'y'
                s3.Object(bucket_sand_box, file.key).delete()

        bucket = "s3://{}/{}/{}".format(bucket_sand_box, table_name, partition_dsc.replace("\'", ""))

        return {
            'Query': "ALTER TABLE {} ADD IF NOT EXISTS PARTITION ({}) location '{}'".format(table_name, partition_dsc,
                                                                                            bucket),
            'Reprocess': table_reprocess
        }
    except Exception as e:
        print("####ERROR#### in get_partitions_queries; because: {}".format(e))


def create_partition(query):
    try:
        athena_client = boto3.client('athena', region_name=environment_region)
        config = {'OutputLocation': tmp_directory}
        athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration=config,
            QueryExecutionContext={
                'Database': database_sandbox
            }
        )
    except Exception as e:
        print("####ERROR#### in create_partition; because: {}".format(e))


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

        sql_query = "SELECT DISTINCT interface_group, interface_name, partition_dsc, interface_type FROM {}.p_cdc WHERE table_status = 'unprocessed' AND interface_group {} {}".format(
            schema_cdc, clause_interface_group, clause_interface_type)

        df = spark.read.format(jdbc_driver) \
            .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
            .option("query", sql_query) \
            .option("aws_iam_role", access_role_read_redshift) \
            .option("tempdir", tmp_directory) \
            .load()

        return df.head(df.count())
    except Exception as e:
        print("####ERROR#### in read_cdc; because: {}".format(e))


def delete_s3_tmp():
    bucket = s3.Bucket(tmp_bucket)
    for obj in bucket.objects.filter(Prefix="sandbox/{}".format(args['interfacegroup'])):
        s3.Object(tmp_bucket, obj.key).delete()


def connect_redshift():
    import pg8000

    host = jdbc_url[jdbc_url.index('//') + 2:
                    jdbc_url.find(':', jdbc_url.index('//') + 2)]
    port = jdbc_url[jdbc_url.find(':', jdbc_url.index('//') + 2) + 1:
                    jdbc_url.find('/', jdbc_url.find(':', jdbc_url.index('//') + 2))]
    database_name = jdbc_url[jdbc_url.find('/', jdbc_url.find(':', jdbc_url.index('//') + 2) + 1) + 1:]

    global redshift_connection

    redshift_connection = pg8000.connect(user=jdbc_user,
                                         host=host,
                                         port=int(port),
                                         database=database_name,
                                         password=jdbc_password)

    redshift_connection.autocommit = True


def get_job_definitions():
    global jdbc_url, jdbc_user, jdbc_password

    try:
        job_conf = client.get_job(JobName=args['JOB_NAME'])

        glue_conn_name = str(job_conf.get("Job", "none").get("Connections", "none").get("Connections", "none")[0])
        glue_conn = client.get_connection(Name=glue_conn_name)

        jdbc_url = glue_conn['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
        jdbc_user = glue_conn['Connection']['ConnectionProperties']['USERNAME']
        jdbc_password = glue_conn['Connection']['ConnectionProperties']['PASSWORD']

    except Exception as e:
        raise Exception("Job Was Unsuccessful; in get_job_definitions #### because {}".format(e))


get_job_definitions()
main()

delete_s3_tmp()
