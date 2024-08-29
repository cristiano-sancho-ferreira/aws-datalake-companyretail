import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'database_source', 'table_source', ])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variaveis
environment_region = glueContext._jvm.AWSConnectionUtils.getRegion()
database_source = args['database_source']
table_source = args['table_source']

# funções
def get_table_map(table_source):
    catalog_map = []
    client = boto3.client('glue', region_name=environment_region)
    response = client.get_table(
        DatabaseName=database_source, Name=table_source)
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

def main ():
    DataSource0 = glueContext.create_dynamic_frame.from_catalog(database=database_source, table_name=table_source, transformation_ctx="DataSource0")

    client = boto3.client('glue', region_name=environment_region)
    table_map = get_table_map(table_source)

    Transform1 = ApplyMapping.apply(frame=DataSource0, mappings=table_map, transformation_ctx="Transform1")

    DataSink0 = glueContext.getSink(path="s3://alelo-bi-dev-datalake/vta_enc/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE",
                                    partitionKeys=["fecha_contable", "centro_cd"], compression="snappy", enableUpdateCatalog=True, transformation_ctx="DataSink0")
    DataSink0.setCatalogInfo(
        catalogDatabase="alelo_bi_dev_datalake", catalogTableName="vta_enc_parquet")
    DataSink0.setFormat("glueparquet")
    DataSink0.writeFrame(Transform1)

    print('Feito com sucesso')
    job.commit()

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)