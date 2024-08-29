#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_vta_prod.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.vta_prod_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.vta_prod;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.vta_prod_flatfile (
centro_cd VARCHAR(10),
nro_caja VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha DATE,
hora VARCHAR(8),
linea_nro VARCHAR(10),
codigo_barra_venta VARCHAR(18),
plu VARCHAR(18),
cantidad double precision,
total_pago_mnt double precision,
iva_mnt double precision,
otros_imp_mnt double precision,
venta_margen_mnt double precision,
precio_digitado_ind VARCHAR(1),
precio_requerido_ind VARCHAR(1),
ingreso_cantidad_ind VARCHAR(1),
tipo_linea VARCHAR(10),
tipo_ingreso VARCHAR(1),
precio_unitario double precision,
tipo_prog_especial VARCHAR(10),
prog_especial_nro VARCHAR(20),
tipo_despacho VARCHAR(10),
tipo_sena VARCHAR(10),
tipo_movi_sena VARCHAR(10),
senia_nro VARCHAR(20),
modifica_inventario_ind VARCHAR(1),
tipo_identifi_supervisor VARCHAR(10),
nro_identif_supervisor VARCHAR(20),
motivo_devolucion_cd VARCHAR(10),
codigo_barra_rebaja VARCHAR(18),
precio_original_rebaja double precision,
motivo_rebaja_cd VARCHAR(10),
motivo_rebaja_desc VARCHAR(100),
anios_garantia double precision,
fecha_contable DATE,
lote_sec_nro  VARCHAR(10),
tipo_movi_rebaja_cd VARCHAR(1),
Venta_bruta double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/vta_prod';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.vta_prod (
nro_caja VARCHAR(10),
transaccion_nro VARCHAR(20),
fecha VARCHAR(10),
hora VARCHAR(8),
linea_nro VARCHAR(10),
codigo_barra_venta VARCHAR(18),
plu VARCHAR(18),
cantidad double precision,
total_pago_mnt double precision,
iva_mnt double precision,
otros_imp_mnt double precision,
venta_margen_mnt double precision,
precio_digitado_ind VARCHAR(1),
precio_requerido_ind VARCHAR(1),
ingreso_cantidad_ind VARCHAR(1),
tipo_linea VARCHAR(10),
tipo_ingreso VARCHAR(1),
precio_unitario double precision,
tipo_prog_especial VARCHAR(10),
prog_especial_nro VARCHAR(20),
tipo_despacho VARCHAR(10),
tipo_sena VARCHAR(10),
tipo_movi_sena VARCHAR(10),
senia_nro VARCHAR(20),
modifica_inventario_ind VARCHAR(1),
tipo_identifi_supervisor VARCHAR(10),
nro_identif_supervisor VARCHAR(20),
motivo_devolucion_cd VARCHAR(10),
codigo_barra_rebaja VARCHAR(18),
precio_original_rebaja double precision,
motivo_rebaja_cd VARCHAR(10),
motivo_rebaja_desc VARCHAR(100),
anios_garantia double precision,
lote_sec_nro  VARCHAR(10),
tipo_movi_rebaja_cd VARCHAR(1),
Venta_bruta double precision)
PARTITIONED BY (fecha_contable VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/vta_prod';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla vta_prod
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.vta_prod" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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