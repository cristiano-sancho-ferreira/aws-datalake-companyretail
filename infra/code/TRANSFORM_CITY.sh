#!/bin/ksh
########################################################################################################################################################
## SHELL      : TRANSFORM_CITY.sh
## Autor      : Silva
## Finalidad  : Este bloque de script es responsable de realizar la lectura de la información de los datos de que están almacenados en las tablas en 
##              formato Parquet en el S3 y grabar en la tabla de CITY, para realizar esta lectura, será utilizado el schema Spectrum. 
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
. ${HOME}/ETL/FUNCTIONS/fn_General.sh

v_schema_stg=`echo ${schema_stg}|awk -F "." '{print $1}'`

psql "${TNS_CONNECT_REDSHIFT}" <<EOF 
\timing

    set search_path to ${v_schema_stg};

	TRUNCATE TABLE ${schema_edw}IN_CITY;
	\p \echo \echo
    
    
	DROP TABLE TMP_IN_CITY;
	\p \echo \echo

	DROP TABLE TMP_R_IN_CITY;
    \p \echo \echo 
    
   ALTER TABLE ${schema_edw}TMP_CITY RENAME TO CITY;
    \p \echo \echo 
	
    DROP TABLE ${schema_edw}TMP_CITY;
    \p \echo \echo 
EOF
	
psql "${TNS_CONNECT_REDSHIFT}" <<EOF 
\set ON_ERROR_STOP on
\timing
 \echo  Lectura de la informacion Spetrum y grabar en la tabla

    set search_path to ${v_schema_stg};
 
  CREATE TABLE TMP_IN_CITY(  City_Cd varchar (50) , City_Name varchar (255) , Territory_Cd varchar (50) , County_Cd varchar (50) ,loadNbr bigint) diststyle all;
    \p \echo \echo 

    INSERT 
    INTO
        ${schema_edw}IN_CITY
        ( City_Cd,City_Name,Territory_Cd,County_Cd,loadNbr) SELECT
            City_Cd,
            City_Name,
            Territory_Cd,
            County_Cd,
            loadNbr 
        FROM
            ${schema_stg}STG_CITY;
    \p \echo \echo 
 
	CREATE TABLE TMP_R_IN_CITY(  City_Cd varchar (50) , City_Name varchar (255) , Territory_Cd varchar (50) , County_Cd varchar (50) ,loadNbr bigint,Group_Rejection_Cd integer) diststyle all;
    \p \echo \echo 
 
    INSERT 
    INTO
        TMP_R_IN_CITY
        SELECT
            City_Cd,
            City_Name,
            Territory_Cd,
            County_Cd,
            loadNbr,
            1 as Group_Rejection_Cd 
        FROM
            ( SELECT
                City_Cd,
                City_Name,
                Territory_Cd,
                County_Cd,
                loadNbr,
                COUNT(*) OVER(PARTITION 
            BY
                City_Cd) COUNT 
            FROM
                ${schema_edw}IN_CITY) RESULT 
        WHERE
            RESULT.COUNT > 1;
    \p \echo \echo 

    INSERT 
    INTO
        TMP_R_IN_CITY
        SELECT
            ${schema_edw}IN_CITY.*,
            2 AS Group_Rejection_Cd 
        FROM
            ${schema_edw}IN_CITY 
        WHERE
            (
                City_Cd
            ) IN (
                SELECT
                    City_Cd 
                FROM
                    ( SELECT
                        RANK() OVER(PARTITION 
                    BY
                        City_Cd 
                    ORDER BY
                        City_Name ) RK,
                        DR.* 
                    FROM
                        ( SELECT
                            DISTINCT City_Cd,
                            City_Name 
                        FROM
                            ${schema_edw}IN_CITY) DR ) DR2 
                    WHERE
                        RK>1);
    \p \echo \echo 
 
    INSERT 
    INTO
        TMP_R_IN_CITY
        SELECT
            DRIVE.*,
            3 AS Group_Rejection_Cd 
        FROM
            ${schema_edw}IN_CITY DRIVE  
        LEFT JOIN
            ${schema_edw}TERRITORY DIMEN 
                ON (
                    DRIVE.Territory_Cd = DIMEN.Territory_Cd
                ) 
        WHERE
            DIMEN.Territory_Cd IS NULL;
    \p \echo \echo 

    INSERT 
    INTO
        TMP_R_IN_CITY
        SELECT
            DRIVE.*,
            4 AS Group_Rejection_Cd 
        FROM
            ${schema_edw}IN_CITY DRIVE  
        LEFT JOIN
            ${schema_edw}COUNTY DIMEN 
                ON (
                    DRIVE.County_Cd = DIMEN.County_Cd
                ) 
        WHERE
            DIMEN.County_Cd IS NULL;
    \p \echo \echo 
 
    analyze tmp_r_in_city;
    \p \echo \echo 
 
    INSERT 
    INTO
        TMP_IN_CITY
        (City_Cd,City_Name,Territory_Cd,County_Cd,loadNbr) SELECT
            DRIVE.City_Cd,
            DRIVE.City_Name,
            DRIVE.Territory_Cd,
            DRIVE.County_Cd,
            DRIVE.loadNbr 
        FROM
            ( SELECT
                COALESCE(City_Cd,
                '') as C_City_Cd,
                City_Cd,
                City_Name,
                Territory_Cd,
                County_Cd,
                loadNbr 
            FROM
                ${schema_edw}IN_CITY) DRIVE 
        LEFT JOIN
            (
                SELECT
                    COALESCE(City_Cd,
                    '') as C_City_Cd,
                    City_Cd,
                    City_Name,
                    Territory_Cd,
                    County_Cd,
                    loadNbr 
                FROM
                    TMP_R_IN_CITY
            ) TMP_R_IN_CITY  
                ON (
                    DRIVE.C_City_Cd = TMP_R_IN_CITY.C_City_Cd
                ) 
        WHERE
            TMP_R_IN_CITY.City_Cd IS NULL;
    \p \echo \echo 
 
    analyze tmp_in_city;
    \p \echo \echo 
 
    DELETE 
    FROM
        ${schema_edw}R_IN_CITY USING TMP_R_IN_CITY  
    WHERE
        (
            (
                R_IN_CITY.City_Cd = TMP_R_IN_CITY.City_Cd
            )
        );
   
	ALTER TABLE ${schema_edw}CITY RENAME TO TMP_CITY;
    \p \echo \echo 

    
	CREATE TABLE ${schema_edw}CITY(  City_Cd varchar (50)  encode zstd , City_Name varchar (255)  encode zstd , Territory_Cd varchar (50)  encode zstd , County_Cd varchar (50)  encode zstd ,loadNbr bigint encode zstd ) diststyle all ;
    \p \echo \echo 
 
    COMMENT 
        ON COLUMN ${schema_edw}CITY.City_Cd is 'The unique code for an instance of the CITY entity.';
    \p \echo \echo 
 
    COMMENT 
        ON COLUMN ${schema_edw}CITY.City_Name is 'The name of a CITY, village, town, etc.';
    \p \echo \echo 
 
    COMMENT 
        ON COLUMN ${schema_edw}CITY.Territory_Cd is 'The unique code for an instance of the TERRITORY entity.';
    \p \echo \echo 
 
    COMMENT 
        ON COLUMN ${schema_edw}CITY.County_Cd is 'The unique code for an instance of the COUNTY entity.';
    \p \echo \echo 
 
    COMMENT 
        ON COLUMN ${schema_edw}CITY.loadNbr is 'Data da ultima atualização do registro';
    \p \echo \echo 

    $(fn_setPermissionsLDMTable ${schema_edw}CITY)
    \p \echo \echo

	INSERT 
    INTO
        ${schema_edw}R_IN_CITY
        (City_Cd,City_Name,Territory_Cd,County_Cd,loadNbr, Group_Rejection_Cd) SELECT
            City_Cd,
            City_Name,
            Territory_Cd,
            County_Cd,
            loadNbr,
            SUM(POW(2,
            Group_Rejection_Cd)) as Group_Rejection_Cd 
        FROM
            ( SELECT
                DISTINCT * 
            FROM
                TMP_R_IN_CITY) X 
        GROUP BY
            City_Cd,
            City_Name,
            Territory_Cd,
            County_Cd,
            loadNbr;
    \p \echo \echo 
 
    DROP TABLE TMP_R_IN_CITY;
    \p \echo \echo 
 
   DELETE 
    FROM
        ${schema_edw}R_IN_CITY USING TMP_IN_CITY  
    WHERE
        (
            (
                ${schema_edw}R_IN_CITY.City_Cd = TMP_IN_CITY.City_Cd
            )
        );
    \p \echo \echo 

  INSERT 
    INTO
        ${schema_edw}CITY
        ( City_Cd,City_Name,Territory_Cd,County_Cd,loadNbr) SELECT
            City_Cd,
            City_Name,
            Territory_Cd,
            County_Cd,
            loadNbr 
        FROM
            TMP_IN_CITY 
        GROUP BY
            City_Cd,
            City_Name,
            Territory_Cd,
            County_Cd,
            loadNbr 
        ORDER BY
            City_Cd;
    \p \echo \echo 
 
    analyze ${schema_edw}city;
    \p \echo \echo 
 
    INSERT 
    INTO
        ${schema_edw}CITY
        SELECT
            TMP_DIM.City_Cd,
            TMP_DIM.City_Name,
            TMP_DIM.Territory_Cd,
            TMP_DIM.County_Cd,
            TMP_DIM.loadNbr 
        FROM
            ${schema_edw}TMP_CITY TMP_DIM 
        LEFT JOIN
            ${schema_edw}CITY DIM 
                ON (
                    TMP_DIM.City_Cd = DIM.City_Cd
                ) 
        WHERE
            DIM.City_Cd IS NULL 
        ORDER BY
            City_Cd;
    \p \echo \echo 

    DROP TABLE TMP_IN_CITY;
    \p \echo \echo 
 
    TRUNCATE TABLE ${schema_stg}STG_CITY;
    \p \echo \echo 

EOF