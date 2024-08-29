#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_empleados.sh
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

source="inf"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.empleados_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.empleados;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.empleados_flatfile(
legajo_cd VARCHAR(20),
nombre_completo VARCHAR(255),
nombre1 VARCHAR(255),
nombre2 VARCHAR(255),
apellido1 VARCHAR(255),
apellido2 VARCHAR(255),
estado VARCHAR(10),
centcosto_cntble_cd VARCHAR(10),
tipodocumento_cd VARCHAR(10),
documento_nro VARCHAR(20),
tipoboca_cd Varchar(100),
tipoboca_desc Varchar(20),
genero_cd VARCHAR(10),
fecha_nacimiento DATE,
fecha_primer_ingreso DATE,
fecha_recontratacion DATE,
fecha_desvinculacion DATE,
posicion_cd VARCHAR(10),
posicion_desc VARCHAR(255),
supervisor_legajo_cd VARCHAR(20),
centro_cd VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/empleados';
    \p \echo \echo 

CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.empleados(
legajo_cd VARCHAR(20),
nombre_completo VARCHAR(255),
nombre1 VARCHAR(255),
nombre2 VARCHAR(255),
apellido1 VARCHAR(255),
apellido2 VARCHAR(255),
estado VARCHAR(10),
centcosto_cntble_cd VARCHAR(10),
tipodocumento_cd VARCHAR(10),
documento_nro VARCHAR(20),
tipoboca_cd Varchar(100),
tipoboca_desc Varchar(20),
genero_cd VARCHAR(10),
fecha_nacimiento VARCHAR(10),
fecha_primer_ingreso VARCHAR(10),
fecha_recontratacion VARCHAR(10),
fecha_desvinculacion VARCHAR(10),
posicion_cd VARCHAR(10),
posicion_desc VARCHAR(255),
supervisor_legajo_cd VARCHAR(20),
centro_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/empleados';
    \p \echo \echo 
    
EOF
