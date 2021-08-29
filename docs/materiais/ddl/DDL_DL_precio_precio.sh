#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_precio_precio.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.precio_precio_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.precio_precio;
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.precio_precio_flatfile (
centro_cd VARCHAR(10),
articulo_cd VARCHAR(18),
umedida_vta_cd VARCHAR(10),
fecha_inicio DATE,
precio_vta_unit_reg_mnt double precision,
iva_precio_vta_unit_reg_mnt double precision,
imp_especifico_unit_reg_mnt double precision,
precio_vta_unit_vig_mnt double precision,
iva_precio_vta_unit_vig_mnt double precision,
imp_especifico_unit_vig_mnt double precision,
precio_vta_neto_unit_reg_mnt double precision,
precio_vta_neto_unit_vig_mnt double precision,
tipo_precio_vta_cd VARCHAR(10),
oferta_cd VARCHAR(18),
canal_distr_cd VARCHAR(10),
codigo_de_barras VARCHAR(18),
pvp_ant double precision,
canal_difusion_cd VARCHAR(10),
clase_precio_vta_cd VARCHAR(10),
fecha_file DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/precio_precio';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.precio_precio (
articulo_cd VARCHAR(18),
umedida_vta_cd VARCHAR(10),
precio_vta_unit_reg_mnt double precision,
iva_precio_vta_unit_reg_mnt double precision,
imp_especifico_unit_reg_mnt double precision,
precio_vta_unit_vig_mnt double precision,
iva_precio_vta_unit_vig_mnt double precision,
imp_especifico_unit_vig_mnt double precision,
precio_vta_neto_unit_reg_mnt double precision,
precio_vta_neto_unit_vig_mnt double precision,
tipo_precio_vta_cd VARCHAR(10),
oferta_cd VARCHAR(18),
canal_distr_cd VARCHAR(10),
codigo_de_barras VARCHAR(18),
pvp_ant double precision,
canal_difusion_cd VARCHAR(10),
clase_precio_vta_cd VARCHAR(10),
fecha_file varchar(10))
PARTITIONED BY (fecha_inicio VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/precio_precio';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla precio_precio
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.precio_precio" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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