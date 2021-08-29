#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_neg_arti.sh
## Autor      : Jackson.Silva(ST IT Consulting)
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              10/07/2018| Código inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="gnx"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.neg_arti_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.neg_arti;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.neg_arti_flatfile(
centro_cd VARCHAR(10),
ejercicio VARCHAR(10),
acuerdo_cd VARCHAR(10),
prov_division_cd  VARCHAR(10),
periodo_contable double precision,
semana INT,
fecha_inicio_liquidacion VARCHAR(10),
fecha_final_liquidacion VARCHAR(10),
numero_documento double precision,
num_condicion VARCHAR(20),
valor_mone_sociedad double precision,
porcentaje_descu double precision,
referencia_pedido VARCHAR(10),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(18),
codigo_interno VARCHAR(4),
fecha_contable VARCHAR(10),
Fecha_doc VARCHAR(10),
proveedor_cd VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/neg_arti';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.neg_arti (
ejercicio VARCHAR(10),
acuerdo_cd VARCHAR(10),
prov_division_cd  VARCHAR(10),
periodo_contable double precision,
semana INT,
fecha_final_liquidacion VARCHAR(10),
numero_documento double precision,
num_condicion VARCHAR(20),
valor_mone_sociedad double precision,
porcentaje_descu double precision,
referencia_pedido VARCHAR(10),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(18),
codigo_interno VARCHAR(4),
fecha_contable VARCHAR(10),
Fecha_doc VARCHAR(10),
proveedor_cd VARCHAR(10))
PARTITIONED BY(fecha_inicio_liquidacion VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/neg_arti';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla neg_arti
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.neg_arti" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

## espera athena terminar de crear particiones
vQUERY_ID=$(echo $QUERY_ID| cut -d'"' -f 4)
vLoop=1
echo -query_id: ${vQUERY_ID}
while [[ 1 -eq $vLoop ]]
do
QUERY_STATE=$(aws athena get-query-execution --region us-east-1 --query-execution-id ${vQUERY_ID})
vQUERY_STATE=$(echo $QUERY_STATE| cut -d'"' -f 10)
echo -status: ${vQUERY_STATE}
case $vQUERY_STATE in
  FAILED|CANCELLED)
   echo #### ERROR #### al crear las particiones
   vLoop=2;;
  SUCCEEDED)
   RC=0
   vLoop=0;;
  *)
   sleep 5
   ;;
esac
done

## apaga carpeta tmp
aws s3 rm ${S3_PATH_APPLICATION} --recursive