#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_catalogo_tipo1.sh
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

source_gnx="gnx"
source_jan="jan"
source_mot="mot"
source_user="user"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_gnx}_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_gnx};
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_jan}_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_jan};
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_mot}_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_mot};
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_user}_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_user};
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_gnx}_flatfile (
catalogo_id VARCHAR(50),
id VARCHAR(50),
description VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source_gnx}/catalogo_tipo1_${source_gnx}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_gnx} (
id VARCHAR(50),
description VARCHAR(255))
PARTITIONED BY(catalogo_id VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/catalogo_tipo1_${source_gnx}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_jan}_flatfile (
catalogo_id VARCHAR(50),
id VARCHAR(50),
description VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source_jan}/catalogo_tipo1_${source_jan}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_jan} (
id VARCHAR(50),
description VARCHAR(255))
PARTITIONED BY(catalogo_id VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/catalogo_tipo1_${source_jan}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_mot}_flatfile (
catalogo_id VARCHAR(50),
id VARCHAR(50),
description VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source_mot}/catalogo_tipo1_${source_mot}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_mot} (
description VARCHAR(255))
PARTITIONED BY(catalogo_id VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/catalogo_tipo1_${source_mot}';
    \p \echo \echo 

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.catalogo_tipo1_${source_user}_flatfile (
catalogo_id VARCHAR(50),
id VARCHAR(50),
description VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source_user}/catalogo_tipo1_${source_user}';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_user} (
id VARCHAR(50),
description VARCHAR(255))
PARTITIONED BY(catalogo_id VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/catalogo_tipo1_${source_user}';
    \p \echo \echo 

EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando view

CREATE OR REPLACE VIEW ${schema_vwdatalake}catalogo_tipo1 AS
SELECT * FROM ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_gnx}
UNION ALL
SELECT * FROM ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_jan}
UNION ALL
SELECT * FROM ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_mot}
UNION ALL
SELECT * FROM ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_user}
WITH NO SCHEMA binding;
    \p \echo \echo 

EOF


## ejecuta comando en el athena 
echo creando particiones de la tabla catalogo_tipo1_${source_gnx}
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_gnx}" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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


## ejecuta comando en el athena 
echo creando particiones de la tabla catalogo_tipo1_${source_jan}
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_jan}" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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

## ejecuta comando en el athena 
echo creando particiones de la tabla catalogo_tipo1_${source_mot}
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_mot}" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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

## ejecuta comando en el athena 
echo creando particiones de la tabla catalogo_tipo1_${source_user}
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.catalogo_tipo1_${source_user}" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
