import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3


args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'interfacegroup', 'company', 'business', 'environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

environment_region = glueContext._jvm.AWSConnectionUtils.getRegion()
interfacegroup = args['interfacegroup']

bucket_data_lake = "{}-{}-{}-lake".format(args['company'], args['business'], args['environment'])
bucket_data_raw = "{}-{}-{}-raw".format(args['company'], args['business'], args['environment'])
bucket_data_arch = "{}-{}-{}-arch".format(args['company'], args['business'], args['environment'])
bucket_data_script = "{}-{}-{}-code".format(args['company'], args['business'], args['environment'])

database_name_lake = "{}_{}_{}_analytics".format(args['company'], args['business'], args['environment'])
database_name_raw = "{}_{}_{}_raw".format(args['company'], args['business'], args['environment'])

has_error = False
error_return = False

s3 = boto3.resource('s3')


def get_table_map(table_flat_file):
    catalog_map = []
    client = boto3.client('glue', region_name=environment_region)
    response = client.get_table(DatabaseName=database_name_raw, Name=table_flat_file)
    for column in response['Table']['StorageDescriptor']['Columns']:
        column_mapping = translate_data_type(column['Name'], column['Type'])
        column_tuple = tuple(column_mapping)
        catalog_map.append(column_tuple)
    return catalog_map


def move_s3_object(source_bucket, source_key, dest_bucket, dest_key):
    s3.Object(dest_bucket, dest_key).copy_from(
        CopySource='{}/{}'.format(source_bucket, source_key), ServerSideEncryption='AES256')
    s3.Object(source_bucket, source_key).delete()


def move_folder_to_archive(table_name, bucket_name):
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix="{}/".format(table_name)):
        move_s3_object(bucket_name, obj.key, bucket_data_arch, obj.key)
    for obj in bucket.objects.filter(Prefix=".ctr/"):
        move_s3_object(bucket_name, obj.key, bucket_data_arch,
                       obj.key.replace('.ctr/', ''))


def translate_data_type(name, data_type):
    if data_type.find("date") >= 0:
        mapping = name, "varchar(10)", name, "varchar(10)"
    else:
        mapping = name, data_type, name, data_type
    return mapping


def get_interfaces(DataParam0):
    df = DataParam0.toDF()

    parameter_list = df.head(df.count())
    if args['interfacegroup'].lower() == "all":
        return parameter_list

    ok = False

    interface_list = []
    for parameter_item in parameter_list:
        if parameter_item.InterfaceGroup.lower() == args['interfacegroup'].lower():
            interface_list.append(parameter_item)
            print("#### OK in get_interfaces #### table parameters; because")
            ok = True

    if not ok:
        error_message = "{} is not a valid interface group".format(
            args['interfacegroup'])
        print("#### ERROR in get_interfaces #### table parameters; because: {}".format(
            error_message))
        global has_error
        has_error = True

    return interface_list


def convert_parquet(table_data_lake, table_flat_file, mode, is_partition, partition_columns):
    try:
        bucket = "s3://{}/{}".format(bucket_data_lake, table_data_lake)
        DataSource0 = glueContext.create_dynamic_frame.from_catalog(database=database_name_raw,
                                                                    table_name=table_flat_file,
                                                                    transformation_ctx="DataSource0")
        client = boto3.client('glue', region_name=environment_region)
        table_map = get_table_map(table_flat_file)
        Transform0 = ApplyMapping.apply(frame=DataSource0,
                                        mappings=table_map,
                                        transformation_ctx="Transform0")
        df_Transform0 = Transform0.toDF()
        if is_partition == "y":
            columns = partition_columns.replace(" ", "").split(",")
            df_Transform0.write.format("parquet").partitionBy(
                columns).mode("overwrite").parquet(bucket)
        else:
            df_Transform0.write.format("parquet").mode(mode).parquet(bucket)
        print(
            f'Execução Glue para importar tabela {table_data_lake} no data lake parquet realizado com sucesso')
        move_folder_to_archive(table_data_lake, bucket_data_raw)

    except Exception as e:
        error_message = "#### ERROR in convert_parquet #### because {}".format(
            e)
        print(error_message)
        has_error = True


def main():
    try:
        current_interface_item = '-not table-'
        global interface_group, bucket_data_arch, bucket_data_raw, has_error, error_return

        # Consultar dados de parametros
        DataParam0 = glueContext.create_dynamic_frame.from_catalog(database=database_name_raw,
                                                                   table_name="parameters",
                                                                   transformation_ctx="DataParam0")

        list_interfaces = get_interfaces(DataParam0)
        for interface_item in list_interfaces:
            current_interface_item = interface_item.TableDataLake.lower()
            if interface_item.Mode.lower() != "overwrite" or interface_item.Mode.lower() != "append":
                interface_group = interface_item.InterfaceGroup

                convert_parquet(interface_item.TableDataLake.lower(),
                                interface_item.TableFlatFile.lower(),
                                interface_item.Mode.lower(),
                                interface_item.IsPartition.lower(),
                                interface_item.PartitionColumn.lower())
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    main()
