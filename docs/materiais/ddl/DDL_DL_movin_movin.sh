#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_movin_movin.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.movin_movin_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.movin_movin;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.movin_movin_flatfile(
movimiento_nro VARCHAR(32),
clase_operacion_cd VARCHAR(10),
fecha_cbte VARCHAR(10),
fecha_mnt VARCHAR(10),
fecha_entrada_cbte VARCHAR(10),
hora_entrada_cbte VARCHAR(8),
doc_referencia_nro VARCHAR(32),
movimiento_linea_nro VARCHAR(32),
clase_movimiento_cd VARCHAR(10),
articulo_cd VARCHAR(18),
centro_cd VARCHAR(10),
almacen_cd VARCHAR(10),
condicion_stock_cd CHAR(3),
proveedor_cd VARCHAR(10),
Signo_ind VARCHAR(3),
costo_total_mnt double precision,
costo_indirecto_mnt double precision,
cantidad double precision,
umedida_base_cd VARCHAR(10),
cantidad_um_entrada double precision,
unimedida_entrada_cd VARCHAR(10),
doc_original_nro VARCHAR(10),
doc_linea_original_nro VARCHAR(10),
centro_costo_cd VARCHAR(10),
orden_nro VARCHAR(20),
activo_fijo_nro VARCHAR(12),
sociedad_cd VARCHAR(10),
articulo_referencia_cd VARCHAR(18),
centro_referencia_cd VARCHAR(10),
almacen_referencia_cd VARCHAR(10),
stock_anterior_qty double precision,
stock_anterior_mnt double precision,
modifica_inventario_ind VARCHAR(3),
movimiento_ind VARCHAR(10),
entrada_ind VARCHAR(10),
consumo VARCHAR(10),
motivo_movimiento VARCHAR(10),
codigo_barra_cd VARCHAR(18),
costo_cpp double precision,
prov_division_cd VARCHAR(10),
usuario_entrada VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/movin_movin';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.movin_movin(
movimiento_nro VARCHAR(32),
clase_operacion_cd VARCHAR(10),
fecha_mnt VARCHAR(10),
fecha_cbte VARCHAR(10),
hora_entrada_cbte VARCHAR(8),
doc_referencia_nro VARCHAR(32),
movimiento_linea_nro VARCHAR(32),
clase_movimiento_cd VARCHAR(10),
articulo_cd VARCHAR(18),
almacen_cd VARCHAR(10),
condicion_stock_cd CHAR(3),
proveedor_cd VARCHAR(10),
Signo_ind VARCHAR(3),
costo_total_mnt double precision,
costo_indirecto_mnt double precision,
cantidad double precision,
umedida_base_cd VARCHAR(10),
cantidad_um_entrada double precision,
unimedida_entrada_cd VARCHAR(10),
doc_original_nro VARCHAR(10),
doc_linea_original_nro VARCHAR(10),
centro_costo_cd VARCHAR(10),
orden_nro VARCHAR(20),
activo_fijo_nro VARCHAR(12),
sociedad_cd VARCHAR(10),
articulo_referencia_cd VARCHAR(18),
centro_referencia_cd VARCHAR(10),
almacen_referencia_cd VARCHAR(10),
stock_anterior_qty double precision,
stock_anterior_mnt double precision,
modifica_inventario_ind VARCHAR(3),
movimiento_ind VARCHAR(10),
entrada_ind VARCHAR(10),
consumo VARCHAR(10),
motivo_movimiento VARCHAR(10),
codigo_barra_cd VARCHAR(18),
costo_cpp double precision,
prov_division_cd VARCHAR(10),
usuario_entrada VARCHAR(20))
PARTITIONED BY (fecha_entrada_cbte VARCHAR(10), centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/movin_movin' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla movin_movin
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.movin_movin" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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