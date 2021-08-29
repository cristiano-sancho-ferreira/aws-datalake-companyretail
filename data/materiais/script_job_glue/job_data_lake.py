# encoding: utf-8
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from botocore.exceptions import ClientError
from time import sleep
from datetime import datetime
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sc._jsc.hadoopConfiguration().set('fs.s3.enableServerSideEncryption', 'true')
sc._jsc.hadoopConfiguration().set('fs.s3.serverSideEncryptionAlgorithm', 'AES256')

database_name_raw = "cencosud_{}_dataraw_{}_{}".format(args['environment'], args['business'], args['country'])
database_name_script = "cencosud_{}_scripts_{}_{}".format(args['environment'], args['business'], args['country'])
database_name_datalake = "cencosud_{}_datalake_{}_{}".format(args['environment'], args['business'], args['country'])
bucket_data_lake = "cencosud.{}.datalake.{}.{}".format(args['environment'], args['business'], args['country'])
bucket_data_raw = ''
bucket_data_arch = ''
folder_data_error = "DataError"

interface_group = 'none'
date_register_flat_file = ''

environment_region = glueContext._jvm.AWSConnectionUtils.getRegion()
glue_conn_name = ''
redshift_database_name = ''
tmp_bucket = "cencosud.{}.datalake.{}.{}".format(args['environment'], args['business'], args['country'])
tmp_directory = "s3://{}/datalake/{}".format(tmp_bucket, args['interfacegroup'])

has_error = False
error_return = False
access_role_read_redshift = args['access_role_read_redshift']

flat_files_names = []


def main():
    current_interface_item = '-not table-'

    try:
        global interface_group, bucket_data_arch, bucket_data_raw, has_error, error_return

        dy_parameter_table = glueContext.create_dynamic_frame.from_catalog(database=database_name_script,
                                                                           table_name="parameters",
                                                                           transformation_ctx="dy_parameter_table")

        list_interfaces = get_interfaces(dy_parameter_table)

        for interface_item in list_interfaces:
            current_interface_item = interface_item.TableDataLake.lower()
            del flat_files_names[:]
            if interface_item.Mode.lower() != "overwrite" or interface_item.Mode.lower() != "append":
                interface_group = interface_item.InterfaceGroup
                bucket_data_raw = "cencosud.{}.dataraw.{}.{}.{}".format(args['environment'], args['business'],
                                                                        args['country'], interface_item.Source.lower())
                bucket_data_arch = "cencosud.{}.dataarch.{}.{}.{}".format(args['environment'], args['business'],
                                                                          args['country'],
                                                                          interface_item.Source.lower())
                count_lines = get_interface_control_count_lines(interface_item.TableDataLake.lower())

                if not has_error:
                    convert_parquet(interface_item.TableDataLake.lower(), interface_item.TableFlatFile.lower(),
                                    interface_item.Mode.lower(),
                                    interface_item.IsPartition.lower(), interface_item.PartitionColumn.lower(),
                                    count_lines)
                else:
                    move_folder_to_error(interface_item.TableDataLake.lower())

                if has_error:
                    error_return = True
                has_error = False

            else:
                error_message = "Mode : {}  not found, please fix it in parameters.json with 'overwrite' or 'append'" \
                    .format(interface_item.Mode)
                print("#### ERROR in main #### because: {}".format(error_message))
                add_control_register(interface_item.TableDataLake.lower(), 9, error_message)
                error_return = True
    except Exception as e:
        error_message = "#### ERROR in main #### because: {}".format(e)
        print(error_message)
        add_control_register(current_interface_item, 9, error_message)
        error_return = True


def get_interfaces(dy_table_parameter):
    df = dy_table_parameter.toDF()

    parameter_list = df.head(df.count())
    if args['interfacegroup'].lower() == "all":
        return parameter_list

    ok = False

    interface_list = []
    for parameter_item in parameter_list:
        if parameter_item.InterfaceGroup.lower() == args['interfacegroup'].lower():
            interface_list.append(parameter_item)
            ok = True

    if not ok:
        error_message = "{} is not a valid interface group".format(args['interfacegroup'])

        print("#### ERROR in get_interfaces #### table parameters; because: {}".format(error_message))
        add_control_register('parameters', 9, error_message)
        global has_error
        has_error = True
    return interface_list


def get_interface_control_count_lines(table_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_data_raw)
    count = 0
    for obj in bucket.objects.filter(Prefix="{}/".format(table_name)):
        if obj.key.lower().endswith('.gz') or obj.key.lower().endswith('.dat'):
            csv_file_name = obj.key[:obj.key.index('.')]
            flat_files_names.append(obj.key[obj.key.rfind('/') + 1:])
            count += get_file_control_count_lines(csv_file_name, table_name)

    for obj in bucket.objects.filter(Prefix="{}/".format(table_name)):
        if obj.key.lower().endswith('.ctr') or obj.key.lower().endswith('.ctl'):
            global has_error
            has_error = True

            error_message = "The CTR file {} doesn't have a .gz pair and will be move it to:'{}/Data_Error/{}' folder" \
                .format(obj.key, bucket_data_raw, table_name)

            print("#### ERROR in get_interface_control_count_lines #### table {}; because: {}"
                  .format(table_name, error_message))

            add_control_register(table_name, 9, error_message)
            move_folder_to_error(table_name)

    return count


def get_file_control_count_lines(key, table_name):
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_data_raw)
        file_control_name = ''

        exists = False
        for obj in bucket.objects.filter(Prefix="{}".format(key)):
            if obj.key.endswith('.ctr'):
                file_control_name = key + '.ctr'
                exists = True
            if obj.key.endswith('.CTR'):
                file_control_name = key + '.CTR'
                exists = True
            if obj.key.endswith('.ctl'):
                file_control_name = key + '.ctl'
                exists = True
            if obj.key.endswith('.CTL'):
                file_control_name = key + '.CTL'
                exists = True

        if exists:
            obj = s3.Object(bucket_data_raw, file_control_name)
            obj_body = obj.get()['Body'].read().decode('utf-8')

            if obj_body.find("|") > 0:
                count_lines = int(obj_body.split("|")[7])
            else:
                count_lines = int(obj_body)

            move_s3_object(bucket_data_raw, file_control_name, bucket_data_raw, ".ctr/{}".format(file_control_name))
            return count_lines
        else:
            error_message = 'file {} does not have a control file'.format(key)
            print("#### ERROR in get_file_control_count_lines #### table {}; because: {}"
                  .format(table_name, error_message))
            add_control_register(table_name, 9, error_message)
            global has_error
            has_error = True
            return 0
    except Exception as e:
        error_message = "Failed to Read {}; Because {}".format(key, e)
        print("#### ERROR in CheckFileExists #### {}".format(error_message))
        add_control_register(table_name, 9, error_message)
        has_error = True
        return 0


def csv_ctr_compare(table_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_data_raw)
    s3_client = boto3.client('s3')
    error_message = "catalog {}_flatfile did not find any files to read".format(table_name)
    csv_name = "-1"
    try:
        for obj_csv in bucket.objects.filter(Prefix="{}/".format(table_name)):
            error_message = ""
            if obj_csv.key.lower().endswith('.gz') or obj_csv.key.lower().endswith('.dat'):
                csv_name = obj_csv.key
                file_name = obj_csv.key[:obj_csv.key.index('.')]
                file_extension = obj_csv.key[obj_csv.key.rfind(".") + 1:]
                compression_type = 'GZIP'
                if file_extension != "gz":
                    compression_type = 'NONE'

                stream_value = s3_client.select_object_content(Bucket=bucket_data_raw, Key=csv_name,
                                                               ExpressionType="SQL",
                                                               Expression="select * from s3object s",
                                                               InputSerialization={'CSV': {},
                                                                                   'CompressionType': compression_type},
                                                               OutputSerialization={'CSV': {}})

                check_quotation_marks = False
                for event_value in stream_value['Payload']:
                    if 'Records' in event_value:
                        file_value = event_value['Records']['Payload'].decode('utf-8')
                        if file_value.find("\"") >= 0:
                            check_quotation_marks = True

                if not check_quotation_marks:

                    stream_count = s3_client.select_object_content(Bucket=bucket_data_raw, Key=csv_name,
                                                                   ExpressionType="SQL",
                                                                   Expression="select count(0) from s3object s",
                                                                   InputSerialization={'CSV': {},
                                                                                       'CompressionType': compression_type},
                                                                   OutputSerialization={'CSV': {}})

                    file_count = 0
                    for event_count in stream_count['Payload']:
                        if 'Records' in event_count:
                            file_count = int(event_count['Records']['Payload'].decode('utf-8'))

                    control_file_count = 0
                    for obj_control in bucket.objects.filter(Prefix=".ctr/{}".format(file_name)):
                        if obj_control.key.lower().endswith('.ctr') or obj_control.key.lower().endswith('.ctl'):
                            control_file_name = obj_control.key
                            obj = s3.Object(bucket_data_raw, control_file_name)
                            obj_body = obj.get()['Body'].read().decode('utf-8')
                            if obj_body.find("|") > 0:
                                control_file_count = int(obj_body.split("|")[7])
                            else:
                                control_file_count = int(obj_body)

                    if control_file_count != file_count:
                        error_message += "\n## The File {} Don't Match With The Control File".format(csv_name)

                else:
                    error_message += "\n## The File {} Has ' \" ' That Corrupted The Table".format(csv_name)

        return error_message

    except Exception as e:
        if csv_name != "-1":
            error_message = "\n## The File {} Corrupted and Could not Read Directory, Because {}".format(csv_name, e)
        else:
            error_message = "\n## The Directory of Table {} is Corrupted and the Files Could not be Read, Because {}" \
                .format(csv_name, e)

        return error_message


def add_flat_file_control(interface_name):
    try:
        client = boto3.client('glue', region_name=environment_region)
        glue_conn = client.get_connection(Name=glue_conn_name)
        p_control = "{}.p_control".format(args['schema_cdc'])
        p_control_flat_file = "{}.p_control_flatfiles".format(args['schema_cdc'])

        jdbc_driver = "com.databricks.spark.redshift"
        jdbc_url = glue_conn['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
        jdbc_user = glue_conn['Connection']['ConnectionProperties']['USERNAME']
        jdbc_password = glue_conn['Connection']['ConnectionProperties']['PASSWORD']

        sql_query = "SELECT loadnbr FROM {} WHERE interface_name = '{}' AND date_process = '{}' AND interface_group = '{}'".format(
            p_control, interface_name, date_register_flat_file, interface_group)

        df = spark.read.format(jdbc_driver) \
            .option("url", jdbc_url + "?user=" + jdbc_user + "&password=" + jdbc_password) \
            .option("query", sql_query) \
            .option("aws_iam_role", access_role_read_redshift) \
            .option("tempdir", tmp_directory) \
            .load()

        load_nbr = df.head().loadnbr

        columns = ["interface_group", 'interface_name', 'date_process', 'loadnbr', 'flatfile_name']
        rows = []
        for names in flat_files_names:
            rows.append([interface_group, interface_name, date_register_flat_file, load_nbr, names])

        df = spark.createDataFrame(rows, columns)
        dy_df = DynamicFrame.fromDF(df, glueContext, "dy_df")

        glueContext.write_dynamic_frame.from_jdbc_conf(frame=dy_df,
                                                       catalog_connection=glue_conn_name,
                                                       connection_options={"dbtable": p_control_flat_file,
                                                                           "database": redshift_database_name},
                                                       redshift_tmp_dir=tmp_directory)

    except Exception as e:
        error_message = '#### ERROR in add_flat_file_control #### because {}'.format(e)
        print(error_message)
        add_control_register(interface_name, 9, error_message)
        global has_error
        has_error = True


def add_control_register(interface_name, return_code, return_dsc):
    p_control = "{}.p_control".format(args['schema_cdc'])

    try:
        columns = ["interface_group", 'interface_name', 'date_process', 'return_code', 'return_dsc']
        global date_register_flat_file
        date_register_flat_file = str(datetime.now())[:19]
        rows = [[interface_group, interface_name, date_register_flat_file, return_code, return_dsc]]

        df = spark.createDataFrame(rows, columns)
        dy_df = DynamicFrame.fromDF(df, glueContext, "dy_df")

        glueContext.write_dynamic_frame.from_jdbc_conf(frame=dy_df,
                                                       catalog_connection=glue_conn_name,
                                                       connection_options={
                                                           "dbtable": p_control,
                                                           "database": redshift_database_name},
                                                       redshift_tmp_dir=tmp_directory)

    except Exception as e:
        error_message = 'Failed to Register in Table {}; because {}'.format(p_control, e)
        print("#### ERROR in add_control_register #### {}".format(error_message))
        global has_error
        has_error = True


def convert_parquet(table_data_lake, table_flat_file, mode, is_partition, partition_columns, count_lines):
    try:
        global has_error

        if count_lines > 0:
            csv_table = glueContext.create_dynamic_frame.from_catalog(database=database_name_raw,
                                                                      table_name=table_flat_file,
                                                                      transformation_ctx="csv_table")

            dy_count = csv_table.count()
            if count_lines == dy_count:

                bucket = "s3://{}/{}".format(bucket_data_lake, table_data_lake)

                table_map = get_table_map(table_flat_file)
                dy_csv = ApplyMapping.apply(frame=csv_table, mappings=table_map, transformation_ctx="dy_csv")

                df_csv = dy_csv.toDF()

                if is_partition == "y":
                    columns = partition_columns.replace(" ", "").split(",")

                    partition_query = get_partitions_query(columns, csv_table, table_data_lake, mode)

                    if not has_error:
                        df_csv.write.partitionBy(columns).mode("append").parquet(bucket)
                        create_partition(table_data_lake, partition_query['Query'])

                    if not has_error:
                        for control in partition_query['Controls']:
                            add_cdc(table_data_lake, control['PartitionDesc'], control['Reprocess'],
                                    control['FullLoad'])
                else:
                    df_csv.write.mode(mode).parquet(bucket)
                    add_cdc(table_data_lake, '-1', 'n', 'y')

                if not has_error:
                    move_folder_to_archive(table_data_lake, bucket_data_raw)
                    add_control_register(table_data_lake, 0, 'Interface Loaded Successfully in DataLake')
                    add_flat_file_control(table_data_lake)
                    print("#### The Table {} Were Successfully loaded ####".format(table_data_lake))
            else:

                files_error_name = csv_ctr_compare(table_data_lake)
                error_message = "the value of the control and the number of rows in the table {} does not match, " \
                                "Because:{}".format(table_data_lake, files_error_name)
                move_folder_to_error(table_data_lake)
                print("#### ERROR in convert_parquet #### {}".format(error_message))
                add_control_register(table_data_lake, 9, error_message)
                has_error = True
        else:
            print("#### The CTR file of the table {} return 0 rows".format(table_data_lake))
            move_folder_to_archive(table_data_lake, bucket_data_raw)

    except Exception as e:
        move_folder_to_error(table_data_lake)
        error_message = "#### ERROR in convert_parquet #### because {}".format(e)
        print(error_message)
        add_control_register(table_data_lake, 9, error_message)
        has_error = True


def get_partitions_query(columns, df_flat_file, table_data_lake, mode):
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_data_lake)
        partition_query = {
            'Query': 'ALTER TABLE {} ADD IF NOT EXISTS '.format(table_data_lake),
            'Controls': []
        }
        list_partitions_date = []
        df_flat_file_only_partitions = df_flat_file.select_fields(columns).toDF().distinct()
        list_rows = df_flat_file_only_partitions.head(df_flat_file_only_partitions.count())

        for row in list_rows:
            index = 0
            table_reprocess = 'n'
            partition_str = ""
            for head in row:
                partition_str += columns[index] + "='" + head + "',"
                index = index + 1

            key = partition_str[:len(partition_str) - 1].replace(",", "/").replace("\'", "")
            for obj in bucket.objects.filter(Prefix="{}/{}/".format(table_data_lake, key)):
                if obj.key.endswith('.parquet'):
                    table_reprocess = 'y'
                    if mode == "overwrite":
                        resilient_delete(bucket_data_lake, obj.key)

            partition_str = partition_str[:len(partition_str) - 1]
            location = "s3://{}/{}/{}".format(bucket_data_lake, table_data_lake,
                                              partition_str.replace(",", "/").replace("\'", ""))
            control = None
            if table_reprocess == 'y':
                control = {
                    'TableDataLake': table_data_lake,
                    'Reprocess': table_reprocess,
                    'PartitionDesc': partition_str,
                    'FullLoad': 'n',
                    'InterfaceGroup': interface_group
                }
            elif table_reprocess == "n" and partition_str[:partition_str.find(',')] not in list_partitions_date:
                list_partitions_date.append(partition_str[:partition_str.find(',')])
                control = {
                    'TableDataLake': table_data_lake,
                    'Reprocess': table_reprocess,
                    'PartitionDesc': partition_str[
                                     :partition_str.find(',') if (partition_str.find(',') >= 0) else None],
                    'FullLoad': 'n',
                    'InterfaceGroup': interface_group
                }

            partition_query['Query'] += "\nPARTITION ({}) LOCATION '{}'".format(partition_str, location)

            if control:
                partition_query['Controls'].append(control)

        return partition_query
    except Exception as e:
        move_folder_to_error(table_data_lake)
        error_message = "#### ERROR in get_partitions_queries #### because {}".format(e)
        print(error_message)
        add_control_register(table_data_lake, 9, error_message)
        global has_error
        has_error = True


def create_partition(table_data_lake, partition_query):
    try:
        client = boto3.client('athena', region_name=environment_region)
        config = {'OutputLocation': tmp_directory}
        client.start_query_execution(
            QueryString=partition_query,
            ResultConfiguration=config,
            QueryExecutionContext={
                'Database': database_name_datalake
            }
        )

    except Exception as e:
        move_folder_to_error(table_data_lake)
        error_message = "#### ERROR in create_partition #### because {}".format(e)
        print(error_message)
        add_control_register(table_data_lake, 9, error_message)
        global has_error
        has_error = True


def resilient_copy(source_bucket, source_key, dest_bucket, dest_key, max_retries=5):
    s3 = boto3.resource('s3')
    for count in xrange(max_retries):
        try:
            s3.Object(dest_bucket, dest_key) \
                .copy_from(CopySource='{}/{}'.format(source_bucket, source_key), ServerSideEncryption='AES256')
            return None
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ["NoSuchKey"]:
                raise exc
            print("#### ERROR in resilient_copy #### Retrying number {} - code: {}".format(count, code))
            if count >= (max_retries - 1):
                raise exc
            sleep(10)


def resilient_delete(source_bucket, source_key, max_retries=5):
    s3 = boto3.resource('s3')
    for count in xrange(max_retries):
        try:
            s3.Object(source_bucket, source_key).delete()
            return None
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ["NoSuchKey"]:
                raise exc
            print("#### ERROR in resilient_delete #### Retrying number {} - code: {}".format(count, code))
            if count >= (max_retries - 1):
                raise exc
            sleep(10)


def move_s3_object(source_bucket, source_key, dest_bucket, dest_key):
    resilient_copy(source_bucket, source_key, dest_bucket, dest_key)
    resilient_delete(source_bucket, source_key)


def move_folder_to_archive(table_name, bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=".ctr/{}".format(table_name)):
        move_s3_object(bucket_name, obj.key, bucket_name, obj.key.replace('.ctr/', ''))
    for obj in bucket.objects.filter(Prefix="{}/".format(table_name)):
        move_s3_object(bucket_name, obj.key, bucket_data_arch, obj.key)


def move_folder_to_error(folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_data_raw)
    for obj in bucket.objects.filter(Prefix=".ctr/{}".format(folder)):
        move_s3_object(bucket_data_raw, obj.key, bucket_data_raw, obj.key.replace('.ctr/', ''))
    for obj in bucket.objects.filter(Prefix="{}/".format(folder)):
        move_s3_object(bucket_data_raw, obj.key, bucket_data_raw, "{}/{}".format(folder_data_error, obj.key))


def get_table_map(table_flat_file):
    client = boto3.client('glue', region_name=environment_region)

    response = client.get_table(DatabaseName=database_name_raw, Name=table_flat_file)

    catalog_map = []

    for column in response['Table']['StorageDescriptor']['Columns']:
        column_mapping = translate_data_type(column['Name'], column['Type'])
        column_tuple = tuple(column_mapping)
        catalog_map.append(column_tuple)

    return catalog_map


def translate_data_type(name, data_type):
    if data_type.find("date") >= 0:
        mapping = name, "varchar(10)", name, "varchar(10)"
    else:
        mapping = name, data_type, name, data_type
    return mapping


def get_interface_name_from_interface_type_dm():
    target_names = ["cuadp_pos_gnx",
                    "cuadp_pos_jan",
                    "plan_ajuste",
                    "plan_plan",
                    "neg_margen",
                    "plan_vtainst",
					"neg_aterrizaje"]
    return target_names


def add_cdc(interface, partition_dsc, is_reprocessing, is_full_load):
    try:
        p_cdc = "{}.p_cdc".format(args['schema_cdc'])
        columns = ['interface_group', 'interface_name', 'date_process', 'partition_dsc', 'is_reprocessing',
                   'is_full_load', 'table_status', 'interface_type']
        date = str(datetime.now())[:19]

        dm_interfaces = get_interface_name_from_interface_type_dm()

        if interface in dm_interfaces:
            interface_type = 'dm'
        else:
            interface_type = 'ldm'

        rows = [[interface_group, interface, date, partition_dsc, is_reprocessing, is_full_load, "unprocessed",
                 interface_type]]

        df = spark.createDataFrame(rows, columns)
        dy_df = DynamicFrame.fromDF(df, glueContext, "dy_df")

        glueContext.write_dynamic_frame.from_jdbc_conf(frame=dy_df,
                                                       catalog_connection=glue_conn_name,
                                                       connection_options={
                                                           "dbtable": p_cdc,
                                                           "database": redshift_database_name},
                                                       redshift_tmp_dir=tmp_directory)

    except Exception as e:
        error_message = '#### ERROR in add_cdc #### because {}'.format(e)
        print(error_message)
        add_control_register(interface, 9, error_message)
        global has_error
        has_error = True


def delete_s3_tmp():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_data_lake)
    for obj in bucket.objects.filter(Prefix="datalake/{}".format(args['interfacegroup'])):
        resilient_delete(bucket_data_lake, obj.key)


def get_redshift_database_name():
    global redshift_database_name

    client = boto3.client('glue', region_name=environment_region)
    glue_conn = client.get_connection(Name=glue_conn_name)
    url_conn = glue_conn['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
    redshift_database_name = url_conn[url_conn.rfind('/') + 1:]


def get_job_definitions():
    global glue_conn_name

    try:
        client = boto3.client('glue', region_name=environment_region)
        job_conf = client.get_job(JobName=args['JOB_NAME'])

        glue_conn_name = str(job_conf.get("Job", "none").get("Connections", "none").get("Connections", "none")[0])

    except Exception as e:
        raise Exception("Job Was Unsuccessful; in get_job_definitions #### because {}".format(e))


get_job_definitions()
get_redshift_database_name()

main()

# se mueve a DataErro y registra en l_control todas las tablas que se generó error
if error_return:
    raise Exception("Job Was Unsuccessful; To See The Errors Check The Log With The \" ####ERROR#### \" Return")
else:
    print("#### Job Executed Successfully ####")

delete_s3_tmp()