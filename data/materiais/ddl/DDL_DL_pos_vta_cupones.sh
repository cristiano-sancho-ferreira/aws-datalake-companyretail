#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pos_vta_cupones.sh
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

source="pos"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pos_vta_cupones_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pos_vta_cupones;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pos_vta_cupones_flatfile (
transaccion_nro VARCHAR(20),
fecha VARCHAR(10),
linea_nro VARCHAR(10),
Consec_dinamica VARCHAR(10),
centro_cd VARCHAR(10),
NegocioId VARCHAR(10),
nro_caja VARCHAR(10),
EAN VARCHAR(20),
FechaHoraCupon VARCHAR(20),
promocion_cd VARCHAR(10),
Valor_cupon double precision,
ind VARCHAR(10),
cupon_emitido_nro VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/pos_vta_cupones';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pos_vta_cupones (
transaccion_nro VARCHAR(20),
linea_nro VARCHAR(10),
Consec_dinamica VARCHAR(10),
NegocioId VARCHAR(10),
nro_caja VARCHAR(10),
EAN VARCHAR(20),
FechaHoraCupon VARCHAR(20),
promocion_cd VARCHAR(10),
Valor_cupon double precision,
ind VARCHAR(10),
cupon_emitido_nro VARCHAR(20))
PARTITIONED BY (fecha VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pos_vta_cupones';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla pos_vta_cupones
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.pos_vta_cupones" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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