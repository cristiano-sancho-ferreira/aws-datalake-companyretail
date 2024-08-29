#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_obs_det.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.obs_det_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.obs_det;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.obs_det_flatfile(
centro_cd VARCHAR(10),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(18),
accion VARCHAR(3),
fecha_obsolescencia VARCHAR(10),
prov_division_cd VARCHAR(20),
uni_mov      double precision,
fec_mov      VARCHAR(10),
por_dec      double precision,
costo_stock  double precision,
dias_anti    double precision,
tip_mov  VARCHAR(3),
dias_cambio double precision,
proveedor_cd VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/obs_det';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.obs_det(
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(18),
prov_division_cd VARCHAR(20),
uni_mov      double precision,
fec_mov      VARCHAR(10),
por_dec      double precision,
costo_stock  double precision,
dias_anti    double precision,
tip_mov  VARCHAR(3),
dias_cambio double precision,
proveedor_cd VARCHAR(20))
PARTITIONED BY (fecha_obsolescencia VARCHAR(10), centro_cd VARCHAR(10), accion VARCHAR(3))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/obs_det' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla obs_det
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.obs_det" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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

