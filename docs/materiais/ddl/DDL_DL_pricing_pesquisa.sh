#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pricing_pesquisa.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pricing_pesquisa_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pricing_pesquisa;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pricing_pesquisa_flatfile(
codigo_bolson double precision,
sector_comer_cd VARCHAR(4),
seccion_comer_cd VARCHAR(4),
grupo_comer_cd VARCHAR(4),
familia_comer_cd VARCHAR(4),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(18),
desc_larga_arti VARCHAR(255),
tipo_sensibilidad VARCHAR(255),
centro_cd VARCHAR(10),
fecha_pesquisa VARCHAR(10),
codigo_competidor double precision,
nombre_competidor VARCHAR(255),
nombre_abreviado_competidor VARCHAR(50),
pvp_competidor double precision,
costo_articulo double precision,
stock_unidades double precision,
estado_punto VARCHAR(4),
pvp_articulo double precision,
estado_decote VARCHAR(3),
estado_oferta VARCHAR(3),
venta_media double precision,
valor_impoconsumo double precision,
valor_iva double precision,
valor_total_impuestos double precision,
marca_propia VARCHAR(3),
codigo_proveedor double precision,
descripcion_proveedor VARCHAR(255),
articulo_cd_agrupado VARCHAR(20),
proveedor_cd VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pricing_pesquisa';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pricing_pesquisa(
codigo_bolson double precision,
sector_comer_cd VARCHAR(4),
seccion_comer_cd VARCHAR(4),
grupo_comer_cd VARCHAR(4),
familia_comer_cd VARCHAR(4),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(18),
desc_larga_arti VARCHAR(255),
tipo_sensibilidad VARCHAR(255),
codigo_competidor double precision,
nombre_competidor VARCHAR(255),
nombre_abreviado_competidor VARCHAR(50),
pvp_competidor double precision,
costo_articulo double precision,
stock_unidades double precision,
estado_punto VARCHAR(4),
pvp_articulo double precision,
estado_decote VARCHAR(3),
estado_oferta VARCHAR(3),
venta_media double precision,
valor_impoconsumo double precision,
valor_iva double precision,
valor_total_impuestos double precision,
marca_propia VARCHAR(3),
codigo_proveedor double precision,
descripcion_proveedor VARCHAR(255),
articulo_cd_agrupado VARCHAR(20),
proveedor_cd VARCHAR(10))
PARTITIONED BY(fecha_pesquisa VARCHAR(10),centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pricing_pesquisa' 
    \p \echo \echo 
    

EOF

## ejecuta comando en el athena
echo creando particiones de la tabla pricing_pesquisa
QUERY_ID=$(aws athena start-query-execution --query-string "MSCK REPAIR TABLE ${SpectrumDatabaseDataLake}.pricing_pesquisa" --result-configuration "OutputLocation=${S3_PATH_APPLICATION},EncryptionConfiguration={EncryptionOption=SSE_S3}" --region us-east-1)

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