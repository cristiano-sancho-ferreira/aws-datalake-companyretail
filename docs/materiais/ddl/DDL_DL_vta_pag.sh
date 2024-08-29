#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_vta_pag.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.vta_pag_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.vta_pag;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.vta_pag_flatfile (
centro_cd VARCHAR(10),
nro_caja VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha DATE,
hora VARCHAR(8),
linea_nro VARCHAR(10),
tipo_operacion VARCHAR(20),
forma_pago_cd VARCHAR(10),
variedad_pago_cd VARCHAR(10),
total_mnt double precision,
monto_donacion double precision,
tipo_instit_donacion VARCHAR(10),
institu_donacion_nro VARCHAR(20),
moneda_pos_cd VARCHAR(10),
tarifa_cambio double precision,
monto_moneda_orginal double precision,
ch_tipo_operacion VARCHAR(10),
ch_fecha_emision DATE,
ch_fecha_cobro DATE,
ch_banco_nro VARCHAR(10),
ch_sucursal_nro VARCHAR(10),
ch_ctacte_nro VARCHAR(20),
ch_cheque_nro VARCHAR(10),
ch_tipo_identifi_emisor VARCHAR(10),
ch_identifi_emisor_nro VARCHAR(20),
cta_cte_nro VARCHAR(10),
cta_cte_exten_nro VARCHAR(10),
tipo_identifi_cliente_ctacte VARCHAR(10),
identifi_cliente_ctacte_nro VARCHAR(20),
tj_marca_cd VARCHAR(20),
tj_tipo_operacion VARCHAR(10),
tj_cuotas VARCHAR(10),
tj_plan_cuotas VARCHAR(10),
codigo_autoriz VARCHAR(10),
lote_trans_nro VARCHAR(10),
ticket_nro  VARCHAR(10),
log_nro VARCHAR(10),
tj_hora VARCHAR(8),
tj_tiempo double precision,
tj_tarjeta_nro VARCHAR(50),
tj_fecha_expiracion VARCHAR(10),
tj_nro_comercio VARCHAR(20),
tj_modo_ingreso VARCHAR(1),
tj_moneda_transaccion_cd VARCHAR(10),
tj_tasa_recargo_pct double precision,
tj_recargo_mnt double precision,
nro_identificador_supervisor VARCHAR(20),
fecha_contable DATE,
lote_sec_nro  VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/vta_pag';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.vta_pag (
nro_caja VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha VARCHAR(10),
hora VARCHAR(8),
linea_nro VARCHAR(10),
tipo_operacion VARCHAR(20),
forma_pago_cd VARCHAR(10),
variedad_pago_cd VARCHAR(10),
total_mnt double precision,
monto_donacion double precision,
tipo_instit_donacion VARCHAR(10),
institu_donacion_nro VARCHAR(20),
moneda_pos_cd VARCHAR(10),
tarifa_cambio double precision,
monto_moneda_orginal double precision,
ch_tipo_operacion VARCHAR(10),
ch_fecha_emision VARCHAR(10),
ch_fecha_cobro VARCHAR(10),
ch_banco_nro VARCHAR(10),
ch_sucursal_nro VARCHAR(10),
ch_ctacte_nro VARCHAR(20),
ch_cheque_nro VARCHAR(10),
ch_tipo_identifi_emisor VARCHAR(10),
ch_identifi_emisor_nro VARCHAR(20),
cta_cte_nro VARCHAR(10),
cta_cte_exten_nro VARCHAR(10),
tipo_identifi_cliente_ctacte VARCHAR(10),
identifi_cliente_ctacte_nro VARCHAR(20),
tj_marca_cd VARCHAR(20),
tj_tipo_operacion VARCHAR(10),
tj_cuotas VARCHAR(10),
tj_plan_cuotas VARCHAR(10),
codigo_autoriz VARCHAR(10),
lote_trans_nro VARCHAR(10),
ticket_nro  VARCHAR(10),
log_nro VARCHAR(10),
tj_hora VARCHAR(8),
tj_tiempo double precision,
tj_tarjeta_nro VARCHAR(50),
tj_fecha_expiracion VARCHAR(10),
tj_nro_comercio VARCHAR(20),
tj_modo_ingreso VARCHAR(1),
tj_moneda_transaccion_cd VARCHAR(10),
tj_tasa_recargo_pct double precision,
tj_recargo_mnt double precision,
nro_identificador_supervisor VARCHAR(20),
lote_sec_nro  VARCHAR(10))
PARTITIONED BY (fecha_contable VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/vta_pag';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla vta_pag
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.vta_pag" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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