#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_stock_stock.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.stock_stock_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.stock_stock;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.stock_stock_flatfile (
articulo_cd VARCHAR(18),
centro_cd VARCHAR(4),
almacen_cd VARCHAR(4),
clave_moneda_cd VARCHAR(5),
umedida_base_cd VARCHAR(3),
mborrado_articulo_cd VARCHAR(1),
control_precio_ind VARCHAR(1),
p_variable_interno_cd double precision,
precio_estandar_cd double precision,
stock_total_valorado_qty double precision,
stock_total_valorado_mnt double precision,
stock_valorado_libre_util_qty double precision,
stock_valorado_libre_util_mnt double precision,
stock_control_calidad_qty double precision,
stock_control_calidad_mnt double precision,
stock_bloqueado_qty double precision,
stock_bloqueado_mnt double precision,
stock_total_lotes_nolibres_qty double precision,
stock_total_lotes_nolibres_mnt double precision,
stock_bloqueado_devol_qty double precision,
stock_bloqueado_devol_mnt double precision,
stock_total_transito_qty double precision,
stock_total_transito_mnt double precision,
fecha_stock VARCHAR(10),
hora_stock VARCHAR(8),
codigo_barras_cd VARCHAR(18),
Estado_de_Stock VARCHAR(1),
estado_articulo_tienda VARCHAR(50),
fecha_ultima_compra VARCHAR(10),
fecha_ultima_venta VARCHAR(10),
fecha_ultima_modificacion VARCHAR(10),
unidades_pendientes double precision,
proveedor_cd VARCHAR(10),
Prov_division_cd VARCHAR(20),
proveedor_ultima_compra_ext VARCHAR(20),
fecha_ultima_compra_externo VARCHAR(10),
estado_arti_cd VARCHAR(4),
concesion_ind VARCHAR(3),
consignacion_ind VARCHAR(3),
importado_ind VARCHAR(3),
marca_propia VARCHAR(1),
precio_vta_unit_vig_mnt double precision,
venta_media double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/stock_stock';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.stock_stock (
articulo_cd VARCHAR(18),
almacen_cd VARCHAR(4),
clave_moneda_cd VARCHAR(5),
umedida_base_cd VARCHAR(3),
mborrado_articulo_cd VARCHAR(1),
control_precio_ind VARCHAR(1),
p_variable_interno_cd double precision,
precio_estandar_cd double precision,
stock_total_valorado_qty double precision,
stock_total_valorado_mnt double precision,
stock_valorado_libre_util_qty double precision,
stock_valorado_libre_util_mnt double precision,
stock_control_calidad_qty double precision,
stock_control_calidad_mnt double precision,
stock_bloqueado_qty double precision,
stock_bloqueado_mnt double precision,
stock_total_lotes_nolibres_qty double precision,
stock_total_lotes_nolibres_mnt double precision,
stock_bloqueado_devol_qty double precision,
stock_bloqueado_devol_mnt double precision,
stock_total_transito_qty double precision,
stock_total_transito_mnt double precision,
hora_stock VARCHAR(8),
codigo_barras_cd VARCHAR(18),
estado_articulo_tienda VARCHAR(50),
fecha_ultima_compra VARCHAR(10),
fecha_ultima_venta VARCHAR(10),
fecha_ultima_modificacion VARCHAR(10),
unidades_pendientes double precision,
proveedor_cd VARCHAR(10),
Prov_division_cd VARCHAR(20),
proveedor_ultima_compra_ext VARCHAR(20),
fecha_ultima_compra_externo VARCHAR(10),
estado_arti_cd VARCHAR(4),
concesion_ind VARCHAR(3),
consignacion_ind VARCHAR(3),
importado_ind VARCHAR(3),
marca_propia VARCHAR(1),
precio_vta_unit_vig_mnt double precision,
venta_media double precision)
PARTITIONED BY (fecha_stock VARCHAR(10), Estado_de_Stock VARCHAR(1), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/stock_stock';
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla stock_stock
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.stock_stock" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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