#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_vta_desc.sh
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

source="jan"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.vta_desc_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.vta_desc;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.vta_desc_flatfile (
centro_cd VARCHAR(10),
nro_caja VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha DATE,
hora VARCHAR(8),
linea_nro VARCHAR(10),
codigo_barra_venta VARCHAR(18),
promocion_qty double precision,
descuento_con_qty double precision,
tipo_descuento_cd VARCHAR(10),
descuento_total_mnt double precision,
iva_mnt double precision,
otros_imp_mnt double precision,
descuento_margen_mnt double precision,
promocion_cd VARCHAR(10),
cupon_nro VARCHAR(20),
participa_margen_ind VARCHAR(1),
tipo_identifi_supervisor VARCHAR(10),
nro_identifi_supervisor VARCHAR(20),
identifi_beneficiario VARCHAR(20),
clase_descuento_cd VARCHAR(10),
fecha_contable Date,
lote_sec_nro  Varchar(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/vta_desc';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.vta_desc (
nro_caja VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha VARCHAR(10),
hora VARCHAR(8),
linea_nro VARCHAR(10),
codigo_barra_venta VARCHAR(18),
promocion_qty double precision,
descuento_con_qty double precision,
tipo_descuento_cd VARCHAR(10),
descuento_total_mnt double precision,
iva_mnt double precision,
otros_imp_mnt double precision,
descuento_margen_mnt double precision,
promocion_cd VARCHAR(10),
cupon_nro VARCHAR(20),
participa_margen_ind VARCHAR(1),
tipo_identifi_supervisor VARCHAR(10),
nro_identifi_supervisor VARCHAR(20),
identifi_beneficiario VARCHAR(20),
clase_descuento_cd VARCHAR(10),
lote_sec_nro  Varchar(10))
PARTITIONED BY (fecha_contable Varchar(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/vta_desc';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla vta_desc
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.vta_desc" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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