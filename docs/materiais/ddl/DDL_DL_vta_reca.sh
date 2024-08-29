#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_vta_reca.sh
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

source="jan"
S3_PATH_APPLICATION="s3://${bucketDataLake}/tmp"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.vta_reca_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.vta_reca;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.vta_reca_flatfile (
centro_cd VARCHAR(10),
caja_nro VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha DATE,
hora VARCHAR(8),
linea_nro VARCHAR(10),
concepto_cobro_cd VARCHAR(10),
tipo_operacion VARCHAR(10),
tipo_pago VARCHAR(10),
total_pago_mnt double precision,
tipo_reacudacion_cd VARCHAR(18),
beneficiario_cd VARCHAR(10),
cuenta_nro VARCHAR(25),
tipo_identifi_titular VARCHAR(10),
nro_identifi_titular VARCHAR(20),
medio_pago VARCHAR(10),
autorizacion_cd VARCHAR(10),
doc_referencia_cd VARCHAR(50),
fecha_contable DATE,
lote_sec_nro  VARCHAR(10),
vlr_comision double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/vta_reca';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.vta_reca (
caja_nro VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha VARCHAR(10),
hora VARCHAR(8),
linea_nro VARCHAR(10),
concepto_cobro_cd VARCHAR(10),
tipo_operacion VARCHAR(10),
tipo_pago VARCHAR(10),
total_pago_mnt double precision,
tipo_reacudacion_cd VARCHAR(18),
beneficiario_cd VARCHAR(10),
cuenta_nro VARCHAR(25),
tipo_identifi_titular VARCHAR(10),
nro_identifi_titular VARCHAR(20),
medio_pago VARCHAR(10),
autorizacion_cd VARCHAR(10),
doc_referencia_cd VARCHAR(50),
lote_sec_nro  VARCHAR(10),
vlr_comision double precision)
PARTITIONED BY (fecha_contable VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/vta_reca';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla vta_reca
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.vta_reca" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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