#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pos_dincom.sh
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

source="pos"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pos_dincom_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pos_dincom;
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pos_dincom_flatfile(
  negocioid varchar(3), 
  dinamicacomercialid varchar(20), 
  tipodinamicacomercialid varchar(3), 
  clasedinamica_comercialid varchar(3), 
  dinamicadescripcion varchar(50), 
  fecha_inicio_dinamica date, 
  fecha_fin_dinamica date, 
  cdpais int, 
  cdtipodescuento varchar(5), 
  cdmodalidad varchar(5), 
  cdmediopago varchar(5), 
  cdbines varchar(5), 
  cdprograma varchar(5), 
  cdsubprogrma varchar(15), 
  cdtipovalor varchar(5), 
  valor double precision, 
  cantidadesxy int, 
  cdseccion varchar(5), 
  cdcodigoevento varchar(20), 
  cdnegocio varchar(5), 
  cdformato varchar(5))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pos_dincom';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pos_dincom(
  negocioid varchar(3), 
  dinamicacomercialid varchar(20), 
  tipodinamicacomercialid varchar(3), 
  clasedinamica_comercialid varchar(3), 
  dinamicadescripcion varchar(50), 
  fecha_inicio_dinamica varchar(10), 
  fecha_fin_dinamica varchar(10), 
  cdpais int, 
  cdtipodescuento varchar(5), 
  cdmodalidad varchar(5), 
  cdmediopago varchar(5), 
  cdbines varchar(5), 
  cdprograma varchar(5), 
  cdsubprogrma varchar(15), 
  cdtipovalor varchar(5), 
  valor double precision, 
  cantidadesxy int, 
  cdseccion varchar(5), 
  cdcodigoevento varchar(20), 
  cdnegocio varchar(5), 
  cdformato varchar(5))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pos_dincom';
    \p \echo \echo 
    
EOF
