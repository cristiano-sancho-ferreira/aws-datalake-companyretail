#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_oc_det.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.oc_det_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.oc_det;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.oc_det_flatfile(
tipo_comprobante VARCHAR(3),
doc_compras_nro VARCHAR(20),
posicion_doc_compras_nro varchar(10),
fecha_modificacion varchar(10),
articulo_cd Varchar(20),
centro_cd varchar(10),
almacen_cd varchar(10),
cantidad double precision,
umedida_cd varchar(10),
umedida_precio_cd varchar(10),
umprp_ump_numerador varchar(10),
umprp_ump_denominador double precision,
ump_umb_numerador double precision,
ump_umb_denominador double precision,
precio_lista_mnt VARCHAR(20),
costo_precio_mnt double precision,
costo_facturado_mnt double precision,
costo_neto_mnt double precision,
precio_lista_iva_mnt double precision,
precio_lis_impu_adicional_mnt double precision,
precio_neto double precision,
cantidad_base double precision,
tipo_impuesto_cd Varchar(10),
unidad_medida_cd Varchar(10),
cliente_nro varchar(20),
plazo_entrega double precision,
peso_neto double precision,
unidad_peso VARCHAR(10),
codigo_barras_cd VARCHAR(18),
peso_bruto double precision,
volumen double precision,
unidad_volumen varchar(10),
borrado_ind VARCHAR(3),
procedencia_cd varchar(10),
entrega_final_ind VARCHAR(3),
factura_final_ind VARCHAR(3),
exceso_limite_pct double precision,
exceso_limite_cant double precision,
exceso_ilimitado_ind VARCHAR(3),
incompleta_limite_pct double precision,
incompleta_limite_cant double precision,
nro_oc_compra_sgc VARCHAR(20),
fecha_generacion VARCHAR(10)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/oc_det';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.oc_det(
tipo_comprobante VARCHAR(3),
doc_compras_nro VARCHAR(20),
posicion_doc_compras_nro varchar(10),
articulo_cd Varchar(20),
centro_cd varchar(10),
cantidad double precision,
umedida_cd varchar(10),
umedida_precio_cd varchar(10),
umprp_ump_numerador varchar(10),
umprp_ump_denominador double precision,
ump_umb_numerador double precision,
ump_umb_denominador double precision,
precio_lista_mnt VARCHAR(20),
costo_precio_mnt double precision,
costo_facturado_mnt double precision,
costo_neto_mnt double precision,
precio_lista_iva_mnt double precision,
precio_lis_impu_adicional_mnt double precision,
precio_neto double precision,
cantidad_base double precision,
tipo_impuesto_cd Varchar(10),
unidad_medida_cd Varchar(10),
cliente_nro varchar(20),
plazo_entrega double precision,
peso_neto double precision,
unidad_peso VARCHAR(10),
codigo_barras_cd VARCHAR(18),
peso_bruto double precision,
volumen double precision,
unidad_volumen varchar(10),
borrado_ind VARCHAR(3),
procedencia_cd varchar(10),
entrega_final_ind VARCHAR(3),
factura_final_ind VARCHAR(3),
exceso_limite_pct double precision,
exceso_limite_cant double precision,
exceso_ilimitado_ind VARCHAR(3),
incompleta_limite_pct double precision,
incompleta_limite_cant double precision,
nro_oc_compra_sgc VARCHAR(20),
fecha_generacion VARCHAR(10))
PARTITIONED BY(fecha_modificacion varchar(10), almacen_cd varchar(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/oc_det' 
    \p \echo \echo 
    
EOF

## ejecuta comando en el athena
echo creando particiones de la tabla oc_det
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.oc_det" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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
