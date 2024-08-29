###########################################################################################
## SHELL      : VTA_IMPDET_MIGRACION.sh
## Autor      : Silva
## Finalidad  : Script que hace inserción de la en migración
## Parámetros : $1 - BEGIN_DATE
##              $2 - END_DATE
## Retorno    : 0 - OK
##              9 - NOK
## Historia   : Fecha     | Descripción
##              ----------|-----------------------------------------------------------------
##              11/10/2018| Código inicial
###########################################################################################
## set -e

BEGIN_DATE=$1
END_DATE=$2

presto-cli --catalog hive  <<EOF
   
INSERT INTO cencosud_desa1_datalake_sm_col_migracion.VTA_IMPDET(
nro_caja 
,transaccion_nro 
,fecha 
,hora 
,linea_nro 
,codigo_barra_venta 
,tipo_impuesto_pos_cd 
,base_imponible_mnt 
,tasa_pct 
,impuesto_mnt 
,lote_sec_nro 
,fecha_contable 
,centro_cd
)
SELECT TAB.*
FROM(SELECT  
		CAST(STL.pos_register_host_cd AS VARCHAR(10)) AS nro_caja
	     ,CAST(STL.sales_transaction_num AS VARCHAR(20)) AS transaccion_nro
		 ,CAST(STL.Tran_Start_Dt AS VARCHAR(10)) AS fecha
	     ,CAST(ST.Tran_Start_Tm AS VARCHAR(8)) AS hora
	     ,CAST(STL.sales_tran_line_num AS VARCHAR(10)) AS linea_nro
	     ,CAST(STL.item_ean_num AS VARCHAR(18)) AS codigo_barra_venta
	     ,CAST('IVA' AS varchar(10)) as tipo_impuesto_pos_cd
	     ,CASE WHEN STL.Sales_tran_line_amt IN(NULL, '') OR STL.Consumption_tax_amt IN(NULL, '') OR STL.tax_pct IN(NULL, '')
			THEN CAST(NULL as Double)
			ELSE (CAST(STL.Sales_tran_line_amt AS DOUBLE) - CAST(STL.Consumption_tax_amt AS DOUBLE))/(1 + (CAST(STL.tax_pct AS DOUBLE) / 100)) 
	      END AS base_imponible_mnt
		,CASE WHEN STL.tax_pct IN(NULL, '')
			THEN CAST(NULL as Double)
			ELSE CAST(STL.tax_pct AS DOUBLE)
		END AS tasa_pct
		,CASE WHEN STL.tax_amt IN(NULL, '')
			THEN CAST(NULL as Double)
			ELSE CAST(STL.tax_amt AS DOUBLE)
		END AS impuesto_mnt
		,CAST('99999' AS varchar(10)) as lote_sec_nro
		,CAST(STL.business_dt AS VARCHAR(10)) AS fecha_contable
              ,CAST(STL.location_host_cd  AS VARCHAR(10)) AS centro_cd
	FROM cencosud_desa1_datalake_sm_col_teradata.sales_transaction_line STL
	LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.sales_transaction ST
		USING(Sales_tran_id)
	WHERE ST.Tipotrans_ind IN ('A','N','D','E','C','S')
       And CAST(STL.business_dt as varchar(10)) between  '${BEGIN_DATE}' and '${END_DATE}'
	And CAST(ST.business_dt as varchar(10)) between  '${BEGIN_DATE}' and '${END_DATE}'
       UNION
    SELECT  
    CAST(STL.pos_register_host_cd AS VARCHAR(10)) AS nro_caja
	,CAST(STL.sales_transaction_num AS VARCHAR(20)) AS transaccion_nro
	,CAST(STL.Tran_Start_Dt AS VARCHAR(10)) AS fecha
	,CAST(ST.Tran_Start_Tm AS VARCHAR(8)) AS hora
	,CAST(STL.sales_tran_line_num AS VARCHAR(10)) AS linea_nro
	,CAST(STL.item_ean_num AS VARCHAR(18)) AS codigo_barra_venta
	,CAST('IPO' AS varchar(10)) as tipo_impuesto_pos_cd
	,CAST(0 as double) AS base_imponible_mnt
	,CAST(NULL AS DOUBLE) as tasa_pct
	,CASE WHEN STL.Consumption_tax_amt IN(NULL,'')
		THEN CAST(NULL as Double)			
              ELSE CAST(STL.Consumption_tax_amt AS DOUBLE)
	END AS impuesto_mnt
	,CAST('99999' AS VARCHAR(10)) as lote_sec_nro
	,CAST(STL.business_dt AS VARCHAR(10)) AS fecha_contable
       ,CAST(STL.location_host_cd  AS VARCHAR(10)) AS centro_cd
	FROM cencosud_desa1_datalake_sm_col_teradata.sales_transaction_line STL
	LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.sales_transaction ST
		USING(Sales_tran_id)
	WHERE ST.Tipotrans_ind IN ('A','N','D','E','C','S') AND CAST(STL.Consumption_tax_amt AS VARCHAR(27)) <> '0'
         And CAST(STL.business_dt as varchar(10)) between  '${BEGIN_DATE}' and '${END_DATE}' 
	  And CAST(ST.business_dt as varchar(10)) between  '${BEGIN_DATE}' and '${END_DATE}' ) TAB;

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_IMPDET.log

exit $?
