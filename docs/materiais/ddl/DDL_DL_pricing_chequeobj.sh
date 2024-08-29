#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pricing_chequeobj.sh
## Autor      : Silva
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pricing_chequeobj_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pricing_chequeobj;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pricing_chequeobj_flatfile(
Centro_Cd VARCHAR(10),
Fecha VARCHAR(10),
Codigo_barras VARCHAR(18),
cedula_chequeador VARCHAR(20),
chequeos_nro double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pricing_chequeobj';
  
CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pricing_chequeobj(
Centro_Cd VARCHAR(10),
Codigo_barras VARCHAR(18),
cedula_chequeador VARCHAR(20),
chequeos_nro double precision)
PARTITIONED BY(Fecha VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pricing_chequeobj';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla pricing_chequeobj
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.pricing_chequeobj" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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

