#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_neg_param.sh
## Autor      : Rafael Melo(4Strategies)
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              24/05/2019| Código inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="user"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.neg_param_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.neg_param;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.neg_param_flatfile (
mes VARCHAR(20),
location_id VARCHAR(20),
item_id VARCHAR(20),
scan_cd VARCHAR(50),
home_number VARCHAR(20),
total_amt DOUBLE PRECISION)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/neg_param';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.neg_param (
location_id VARCHAR(20),
item_id VARCHAR(20),
scan_cd VARCHAR(50),
home_number VARCHAR(20),
total_amt DOUBLE PRECISION)
PARTITIONED BY(mes VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/neg_param';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla neg_param
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.neg_param" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
