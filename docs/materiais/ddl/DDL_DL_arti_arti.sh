#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_arti_arti.sh
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

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.arti_arti_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.arti_arti;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.arti_arti_flatfile (
operacion_cd VARCHAR(1), 
articulo_cd VARCHAR(18), 
fecha_creacion date, 
fecha_modif date, 
borrado_logico VARCHAR(4), 
tipo_arti_cd VARCHAR(4), 
nro_antiguo_arti VARCHAR(18), 
base_umedida_cd VARCHAR(3), 
pedido_umedida_cd VARCHAR(3), 
sector_comer_cd VARCHAR(2), 
tipo_estacion VARCHAR(4), 
marca_cd VARCHAR(18), 
fecha_inicio date, 
ano_estacion VARCHAR(4), 
articulo_categoria VARCHAR(4), 
estado_arti_cd VARCHAR(4), 
contenido_umedida_cd VARCHAR(3), 
cont_neto double precision, 
qty_base_comparacion double precision, 
cont_bruto double precision, 
articulo_padre_cd VARCHAR(18), 
ticket_desc VARCHAR(255), 
desc_corta_arti VARCHAR(255), 
desc_larga_arti VARCHAR(255), 
venta_umedida_cd VARCHAR(3), 
estado_venta VARCHAR(10), 
comodato_ind VARCHAR(3), 
concesion_ind VARCHAR(3), 
consignacion_ind VARCHAR(3), 
compra_a_pedido_ind VARCHAR(3), 
pais_cd VARCHAR(3), 
importado_ind VARCHAR(3), 
perfil_aprov_cd VARCHAR(10), 
status_arti_cad_distri VARCHAR(5), 
status_creacion_cd VARCHAR(10), 
precio_articulo_cd varchar(255), 
status_arti_cad_distri_fec date, 
garantia_extendida VARCHAR(1), 
venta_asistida VARCHAR(1), 
impoconsumo VARCHAR(1), 
retefuente double precision, 
temporada VARCHAR(1), 
saludables_1 VARCHAR(35), 
marca_propia VARCHAR(1), 
factor_redondeo double precision, 
pack VARCHAR(1), 
articulos_fraccionados double precision, 
clave_surtido_nacional VARCHAR(1), 
unidades_de_pack int,
codigo_barra_cd VARCHAR(18),
codigo_barra_completo VARCHAR(18))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/arti_arti';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.arti_arti (
operacion_cd VARCHAR(1), 
articulo_cd VARCHAR(18), 
fecha_creacion VARCHAR(10), 
fecha_modif VARCHAR(10), 
borrado_logico VARCHAR(4), 
tipo_arti_cd VARCHAR(4), 
nro_antiguo_arti VARCHAR(18), 
base_umedida_cd VARCHAR(3), 
pedido_umedida_cd VARCHAR(3), 
sector_comer_cd VARCHAR(2), 
tipo_estacion VARCHAR(4), 
marca_cd VARCHAR(18), 
fecha_inicio VARCHAR(10), 
ano_estacion VARCHAR(4), 
articulo_categoria VARCHAR(4), 
estado_arti_cd VARCHAR(4), 
contenido_umedida_cd VARCHAR(3), 
cont_neto double precision, 
qty_base_comparacion double precision, 
cont_bruto double precision, 
articulo_padre_cd VARCHAR(18), 
ticket_desc VARCHAR(255), 
desc_corta_arti VARCHAR(255), 
desc_larga_arti VARCHAR(255), 
venta_umedida_cd VARCHAR(3), 
estado_venta VARCHAR(10), 
comodato_ind VARCHAR(3), 
concesion_ind VARCHAR(3), 
consignacion_ind VARCHAR(3), 
compra_a_pedido_ind VARCHAR(3), 
pais_cd VARCHAR(3), 
importado_ind VARCHAR(3), 
perfil_aprov_cd VARCHAR(10), 
status_arti_cad_distri VARCHAR(5), 
status_creacion_cd VARCHAR(10), 
precio_articulo_cd varchar(255), 
status_arti_cad_distri_fec VARCHAR(10), 
garantia_extendida VARCHAR(1), 
venta_asistida VARCHAR(1), 
impoconsumo VARCHAR(1), 
retefuente double precision, 
temporada VARCHAR(1), 
saludables_1 VARCHAR(35), 
marca_propia VARCHAR(1), 
factor_redondeo double precision, 
pack VARCHAR(1), 
articulos_fraccionados double precision, 
clave_surtido_nacional VARCHAR(1), 
unidades_de_pack int,
codigo_barra_cd VARCHAR(18),
codigo_barra_completo VARCHAR(18))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/arti_arti';
    \p \echo \echo 
 
EOF
