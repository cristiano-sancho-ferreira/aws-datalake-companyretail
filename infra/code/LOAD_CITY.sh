#!/bin/ksh
########################################################################################################################################################
## SHELL      : CITY.sh
## Autor      : Silva
## Finalidad  : Este bloque de script es responsable de realizar la lectura de la información de los datos de que están almacenados en las tablas en 
##              formato Parquet en el S3 y grabar en la tabla de City, para realizar esta lectura, será utilizado el schema Spectrum. 
## Parámetros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecución
## Historia   : Fecha     | Descripción
##              ----------|-----------------------------------------------------------------
##              17/08/2018| Código inicial
########################################################################################################################################################
##set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
	TRUNCATE TABLE ${schema_stg}STG_CITY;
	\p \echo \echo 
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo Lectura de la información Spectrum y grabar en la tabla

 INSERT 
    INTO
        ${schema_stg}STG_CITY
        (      City_cd,      City_Name,      Territory_Cd,      County_Cd,  LoadNbr )  SELECT
            CAST(city_cd AS VARCHAR(50)),
            CAST(city_name AS VARCHAR(255)),
            CAST(territory_cd AS VARCHAR(50)),
            CAST(county_cd AS VARCHAR(50)),
            ${vLoadNBR}	as LoadNbr		
        FROM
            ${schema_datalake}centro_city  
        UNION
        SELECT
            '-1',
            'No Informado',
            '-1',
            '-1',
            ${vLoadNBR}	as LoadNbr;
    \p \echo \echo 

EOF