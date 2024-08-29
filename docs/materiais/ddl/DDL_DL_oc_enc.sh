#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_oc_enc.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.oc_enc_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.oc_enc;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.oc_enc_flatfile(
tipo_comprobante VARCHAR(3),
nro_doc_compras VARCHAR(20),
sociedad_cd VARCHAR(4),
tipo_doc_compras VARCHAR(3),
clase_pedido_cd VARCHAR(4),
transpaso_sucursal_ind VARCHAR(3),
fecha_modificacion varchar(10),
proveedor_cd VARCHAR(10),
org_compras VARCHAR(4),
grupo_compra VARCHAR(3),
moneda_cd varchar(10),
tipo_cambio_mone double precision,
fija_tipo_cambio VARCHAR(3),
fecha_generacion varchar(10),
fecha_liberacion varchar(10),
fecha_inc_rangoentrega varchar(10),
fecha_fin_rangoentrega varchar(10),
centro_transf_interna VARCHAR(4),
terminos_importacion_1 VARCHAR(3),
terminos_importacion_2 VARCHAR(28),
nron_condi_doc VARCHAR(10),
esquema_precios VARCHAR(6),
fact_proveedor_cd VARCHAR(10),
importacion_nro VARCHAR(10),
grupo_liberacion_cd VARCHAR(3),
grupo_liberacion_desc VARCHAR(255),
estrategia_libera_cd VARCHAR(3),
estrategia_libera_desc VARCHAR(255),
doc_libera_ind_cd VARCHAR(3),
doc_libera_ind_desc VARCHAR(255),
estado_libera_cd VARCHAR(8),
estado_libera_desc VARCHAR(255),
libera_incompleta VARCHAR(3),
regionsgc_cd VARCHAR( 4 ),
prov_division_cd VARCHAR(10),
plan_pago_cd VARCHAR(10),
comprador_cd VARCHAR(10),
comprador_apellido VARCHAR(255),
comprador_nombre VARCHAR(255),
valor_oc_mnt double precision,
descuento_oc_mnt double precision,
valor_neto_oc_mnt double precision,
nro_oc_compra_sgc VARCHAR(20),
promocion_cd VARCHAR(10),
Issuing_Location_Id varchar(10),
former_location_id varchar(10),
former_Vendor_PO_Id VARCHAR(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/oc_enc';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.oc_enc(
tipo_comprobante VARCHAR(3),
nro_doc_compras VARCHAR(20),
sociedad_cd VARCHAR(4),
tipo_doc_compras VARCHAR(3),
clase_pedido_cd VARCHAR(4),
transpaso_sucursal_ind VARCHAR(3),
proveedor_cd VARCHAR(10),
org_compras VARCHAR(4),
grupo_compra VARCHAR(3),
moneda_cd varchar(10),
tipo_cambio_mone double precision,
fija_tipo_cambio VARCHAR(3),
fecha_generacion VARCHAR(10),
fecha_liberacion varchar(10),
fecha_inc_rangoentrega varchar(10),
fecha_fin_rangoentrega varchar(10),
terminos_importacion_1 VARCHAR(3),
terminos_importacion_2 VARCHAR(28),
nron_condi_doc VARCHAR(10),
esquema_precios VARCHAR(6),
fact_proveedor_cd VARCHAR(10),
importacion_nro VARCHAR(10),
grupo_liberacion_cd VARCHAR(3),
grupo_liberacion_desc VARCHAR(255),
estrategia_libera_cd VARCHAR(3),
estrategia_libera_desc VARCHAR(255),
doc_libera_ind_cd VARCHAR(3),
doc_libera_ind_desc VARCHAR(255),
estado_libera_cd VARCHAR(8),
estado_libera_desc VARCHAR(255),
libera_incompleta VARCHAR(3),
regionsgc_cd VARCHAR( 4 ),
prov_division_cd VARCHAR(10),
plan_pago_cd VARCHAR(10),
comprador_cd VARCHAR(10),
comprador_apellido VARCHAR(255),
comprador_nombre VARCHAR(255),
valor_oc_mnt double precision,
descuento_oc_mnt double precision,
valor_neto_oc_mnt double precision,
nro_oc_compra_sgc VARCHAR(20),
promocion_cd VARCHAR(10),
Issuing_Location_Id varchar(10),
former_location_id varchar(10),
former_Vendor_PO_Id VARCHAR(20))
PARTITIONED BY (fecha_modificacion varchar(10), centro_transf_interna VARCHAR(4))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/oc_enc' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla oc_enc
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.oc_enc" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
