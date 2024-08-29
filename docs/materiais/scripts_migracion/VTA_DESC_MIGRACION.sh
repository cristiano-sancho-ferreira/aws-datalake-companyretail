###########################################################################################
## SHELL      : VTA_DESC_migracion.sh
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
   
INSERT INTO cencosud_desa1_datalake_sm_col_migracion.VTA_DESC(
nro_caja
,transaccion_nro
,fecha 
,hora 
,linea_nro 
,codigo_barra_venta 
,promocion_qty 
,descuento_con_qty 
,tipo_descuento_cd 
,descuento_total_mnt 
,iva_mnt 
,otros_imp_mnt 
,descuento_margen_mnt 
,promocion_cd
,cupon_nro 
,participa_margen_ind 
,tipo_identifi_supervisor 
,nro_identifi_supervisor 
,identifi_beneficiario 
,clase_descuento_cd 
,lote_sec_nro 
,fecha_contable 
,centro_cd
)
SELECT DISTINCT
	 CAST(DISC.Pos_Register_Host_CD AS VARCHAR(10)) AS nro_caja
	,CAST(DISC.Sales_Transaction_Num AS VARCHAR(20)) AS transaccion_nro
	,CAST(TO_DATE(DISC.Tran_Start_Dt, 'yyyymmdd') AS VARCHAR(10)) AS fecha
	,CAST(DISC.Tran_Start_Tm AS VARCHAR(8)) AS hora
	,CAST(DISC.Sales_Tran_Line_Num AS VARCHAR(10)) AS linea_nro
	,CAST(DISC.Item_EAN_Num AS VARCHAR(18)) AS codigo_barra_venta
	,CAST(CASE WHEN TLINE.Item_Qty IN('')
		THEN NULL
		ELSE TLINE.Item_Qty
	END AS DOUBLE) AS promocion_qty
	,CAST(CASE WHEN TLINE.Item_Qty IN('')
		THEN NULL
		ELSE TLINE.Item_Qty
	END AS DOUBLE) AS descuento_con_qty
	,CAST(NULL  AS VARCHAR(10)) AS tipo_descuento_cd
	,CAST(CASE WHEN DISC.Discount_Amt IN('')
		THEN NULL
		ELSE DISC.Discount_Amt
	END AS DOUBLE) AS descuento_total_mnt
	,CAST(0 AS DOUBLE) as  iva_mnt
	,CAST(0 AS  DOUBLE) as otros_imp_mnt
	,CAST(0 AS  DOUBLE) as descuento_margen_mnt
	,CAST(CASE WHEN 
		 LENGTH(PROMO_OFFER.Promo_Offer_Desc_1) = 43
		 AND SUBSTR(PROMO_OFFER.Promo_Offer_Desc_1, 1, 1) = '1'
		 AND PROMO_OFFER.Promo_Offer_Desc_1 NOT LIKE '% %'
		THEN PROMO_OFFER.Promo_Offer_Desc_1
		ELSE PROMO_OFFER.Promo_Offer_Host_CD 
	END AS VARCHAR(10)) AS promocion_cd
	,CAST(NULL AS varchar(20)) as cupon_nro
    ,CAST(CASE WHEN SUBSTR(TRANSFORM_Promo_cd.Promocion_cd, 3, 3) = '002' 
		THEN 'F'
		ELSE 'C'
	END AS VARCHAR(1)) AS participa_margen_ind
	,CAST(ASSOCIATE.Party_Identification_Type_Cd AS VARCHAR(10)) AS tipo_identifi_supervisor
	,CAST(ASSOCIATE.Party_Identification_Num AS VARCHAR(20)) AS nro_identifi_supervisor
	,CAST(DISC.Party_Identification_Num AS VARCHAR(20)) AS identifi_beneficiario
	,CAST('C' AS VARCHAR(10)) clase_descuento_cd
	,CAST(date_format(localtimestamp, '%y%m%d%H%i') AS VARCHAR(10)) AS lote_sec_nro
	,CAST(to_date(DISC.Business_Dt, 'yyyymmdd') AS VARCHAR(10)) AS fecha_contable
	,CAST(DISC.Location_Host_CD AS VARCHAR(10)) AS centro_cd
FROM cencosud_desa1_datalake_sm_col_teradata.SALES_TRAN_DISCOUNT_LINE DISC
LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.SALES_TRANSACTION_LINE TLINE
	ON (TLINE.Sales_Tran_Id = DISC.Sales_Tran_Id 
	AND TLINE.Sales_Tran_Line_Num = DISC.Sales_Tran_Line_Num)
LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.ASSOCIATE
	ON TLINE.Manager_Associate_ID = ASSOCIATE.Party_ID
LEFT JOIN cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER PROMO_OFFER
	USING (Promo_Offer_Id)
LEFT JOIN (SELECT 
			(CASE WHEN 
			LENGTH(PO.Promo_Offer_Desc_1) = 43
			AND SUBSTR(PO.Promo_Offer_Desc_1, 1, 1) = '1'
			AND PO.Promo_Offer_Desc_1 NOT LIKE '% %'
				THEN PO.Promo_Offer_Desc_1
				ELSE PO.Promo_Offer_Host_CD
			END) AS promocion_cd, 
			Promo_Offer_Id
			FROM cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER PO) TRANSFORM_Promo_cd
	ON TRANSFORM_Promo_cd.Promo_Offer_Id = DISC.Promo_Offer_Id
  WHERE (CAST((Substr(DISC.business_dt,1,4)||'-'||Substr(DISC.business_dt,5,2)||'-'||Substr(DISC.business_dt,7,2)) as varchar(10))  BETWEEN '${BEGIN_DATE}' AND '${END_DATE}') 
   AND (CAST(TLINE.business_dt  AS VARCHAR(10)) BETWEEN '${BEGIN_DATE}' AND '${END_DATE}');

exit;
EOF

## log
echo "${BEGIN_DATE}" >> VTA_DESC.log

exit $?
