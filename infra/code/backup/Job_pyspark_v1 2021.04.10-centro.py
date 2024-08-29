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

# funções main
def main ():
    
    #datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "alelo_bi_dev_dataraw", table_name = "centro_centro_flatfile", transformation_ctx = "datasource0")
    DataSource0 = glueContext.create_dynamic_frame.from_catalog(database=database_source, 
                                                                table_name=table_source, 
                                                                transformation_ctx="DataSource0")

    client = boto3.client('glue', region_name=environment_region)
    table_map = get_table_map(table_source)

    #applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("operacion_cd", "string", "operacion_cd", "string"), ("centro_cd", "string", "centro_cd", "string"), ("centro_nombre", "string", "centro_nombre", "string"), ("nro_cliente", "string", "nro_cliente", "string"), ("direccion", "string", "direccion", "string"), ("codigo_postal", "string", "codigo_postal", "string"), ("ciudad", "string", "ciudad", "string"), ("orga_compras", "string", "orga_compras", "string"), ("orga_ventas", "string", "orga_ventas", "string"), ("clave_pais", "string", "clave_pais", "string"), ("region_cd", "string", "region_cd", "string"), ("cadena_cd", "string", "cadena_cd", "string"), ("codigo_municipal", "string", "codigo_municipal", "string"), ("direccion_local", "string", "direccion_local", "string"), ("canal_distribucion", "string", "canal_distribucion", "string"), ("tipo_centro", "string", "tipo_centro", "string"), ("proveedor_regular_ind", "string", "proveedor_regular_ind", "string"), ("zona_costo", "string", "zona_costo", "string"), ("id_ant_centro", "string", "id_ant_centro", "string"), ("regionsgc_cd", "string", "regionsgc_cd", "string"), ("formato_local", "string", "formato_local", "string"), ("zona_comercial", "string", "zona_comercial", "string"), ("estado_local", "string", "estado_local", "string"), ("fecha_inc_actividad", "date", "fecha_inc_actividad", "date"), ("fecha_fin_actividad", "date", "fecha_fin_actividad", "date"), ("superf_salon", "string", "superf_salon", "string"), ("sociedad_cd", "string", "sociedad_cd", "string"), ("virtual", "string", "virtual", "string"), ("descripcion_corta", "string", "descripcion_corta", "string")], transformation_ctx = "applymapping1")
    Transform0 = ApplyMapping.apply(frame=DataSource0, 
                                    mappings=table_map, 
                                    transformation_ctx="Transform0")

    #resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
    #dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
    #datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://alelo-bi-dev-datalake/centro_centro"}, format = "parquet", transformation_ctx = "datasink4")
    
    DataSink0 = glueContext.getSink(path="s3://alelo-bi-dev-datalake/centro_centro/", 
                                    connection_type="s3", 
                                    updateBehavior="UPDATE_IN_DATABASE",
                                    compression="snappy", 
                                    enableUpdateCatalog=True, 
                                    transformation_ctx="DataSink0")

    DataSink0.setCatalogInfo(catalogDatabase="alelo_bi_dev_datalake", 
                             catalogTableName="centro_centro_parquet")
    DataSink0.setFormat("glueparquet")
    DataSink0.writeFrame(Transform0)

    print('Execução Glue para importar tabela no data lake parquet realizado com sucesso')
    job.commit()


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(ex)
