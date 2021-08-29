#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_gond_gond.sh
## Autor      : Jackson.Silva(ST IT Consulting)
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              10/07/2018| Codigo inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.gond_gond_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.gond_gond;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.gond_gond_flatfile(
pais VARCHAR(3),
centro_cd VARCHAR(10),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(20),
umv VARCHAR(5),
caras  VARCHAR(10),
capacidad VARCHAR(10),
capacidad_Sugerida VARCHAR(10),
tipo_de_fleje VARCHAR(10),
cantidad_de_etiquetas VARCHAR(4),
tipo_de_ubicacion VARCHAR(3),
ubicacion VARCHAR(3),
seccion VARCHAR(4),
pasillo  VARCHAR(4),
lineal  VARCHAR(3),
metro  VARCHAR(4),
altura  VARCHAR(5),
caja VARCHAR(3),
cabecera VARCHAR(4),
orientacion VARCHAR(3),
fecha_desde VARCHAR(10),
usuario VARCHAR(20),
tipo_novedad VARCHAR(3),
ancho VARCHAR(10),
profundidad VARCHAR(10),
bandejas VARCHAR(10),
estado VARCHAR(3),
parametrizacion_gondola VARCHAR(3))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/gond_gond';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.gond_gond(
pais VARCHAR(3),
centro_cd VARCHAR(10),
articulo_cd VARCHAR(18),
codigo_barras_cd VARCHAR(20),
umv VARCHAR(5),
caras  VARCHAR(10),
capacidad VARCHAR(10),
capacidad_Sugerida VARCHAR(10),
tipo_de_fleje VARCHAR(10),
cantidad_de_etiquetas VARCHAR(4),
tipo_de_ubicacion VARCHAR(3),
ubicacion VARCHAR(3),
seccion VARCHAR(4),
pasillo  VARCHAR(4),
lineal  VARCHAR(3),
metro  VARCHAR(4),
altura  VARCHAR(5),
caja VARCHAR(3),
cabecera VARCHAR(4),
orientacion VARCHAR(3),
fecha_desde VARCHAR(10),
usuario VARCHAR(20),
tipo_novedad VARCHAR(3),
ancho VARCHAR(10),
profundidad VARCHAR(10),
bandejas VARCHAR(10),
estado VARCHAR(3),
parametrizacion_gondola VARCHAR(3))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/gond_gond' 
    \p \echo \echo 
    
EOF
