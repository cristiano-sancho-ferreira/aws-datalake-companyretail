#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_cotiz_det.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.cotiz_det_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.cotiz_det;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.cotiz_det_flatfile(
id_transaccion  VARCHAR(50),
linea_nro VARCHAR(50),
codigo_barras VARCHAR(20),
articulo_cd VARCHAR(18),
importe_unitario double precision,
importe_uni_s_desc double precision,
cantidad double precision,
precio_uni_cobrado double precision,
cantidad_cobrada double precision,
observaciones VARCHAR(255),
porc_descuento double precision,
porc_iva double precision,
nro_lista VARCHAR(50),
tipo_lista VARCHAR(50),
local_despacho_cd VARCHAR(10),
costo_unitario double precision,
importe_descuento double precision,
autoriza_cd VARCHAR(50),
tipo_id_autorizador VARCHAR(10),
id_autorizador_operador VARCHAR(50),
nombre_autorizante_dto VARCHAR(50),
fecha_autorizacion VARCHAR(10),
bonificacion double precision,
umedida_cd Varchar(20),
origen_descuento_cd VARCHAR(10),
tipo_despacho VARCHAR(10),
tipo_articulo_pres VARCHAR(10),
subtipo_articulo_pres VARCHAR(10),
fecha_entrega VARCHAR(10),
tipo_retiro VARCHAR(5),
doc_compras_nro VARCHAR(10),
fecha_inicio_trans VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/cotiz_det';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.cotiz_det(
id_transaccion  VARCHAR(50),
linea_nro VARCHAR(50),
codigo_barras VARCHAR(20),
articulo_cd VARCHAR(18),
importe_unitario double precision,
importe_uni_s_desc double precision,
cantidad double precision,
precio_uni_cobrado double precision,
cantidad_cobrada double precision,
observaciones VARCHAR(255),
porc_descuento double precision,
porc_iva double precision,
nro_lista VARCHAR(50),
tipo_lista VARCHAR(50),
costo_unitario double precision,
importe_descuento double precision,
autoriza_cd VARCHAR(50),
tipo_id_autorizador VARCHAR(10),
id_autorizador_operador VARCHAR(50),
nombre_autorizante_dto VARCHAR(50),
fecha_autorizacion VARCHAR(10),
bonificacion double precision,
umedida_cd Varchar(20),
origen_descuento_cd VARCHAR(10),
tipo_despacho VARCHAR(10),
tipo_articulo_pres VARCHAR(10),
subtipo_articulo_pres VARCHAR(10),
fecha_entrega VARCHAR(10),
tipo_retiro VARCHAR(5),
doc_compras_nro VARCHAR(10))
PARTITIONED BY(fecha_inicio_trans VARCHAR(10), local_despacho_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/cotiz_det' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla cotiz_det
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.cotiz_det" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
