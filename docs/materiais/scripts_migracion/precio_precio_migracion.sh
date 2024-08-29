###########################################################################################
## SHELL      : precio_precio_migracion.sh
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
   
Insert into cencosud_desa1_datalake_sm_col_migracion.precio_precio (
articulo_cd 
,umedida_vta_cd 
,precio_vta_unit_reg_mnt 
,iva_precio_vta_unit_reg_mnt 
,imp_especifico_unit_reg_mnt 
,precio_vta_unit_vig_mnt 
,iva_precio_vta_unit_vig_mnt 
,imp_especifico_unit_vig_mnt 
,precio_vta_neto_unit_reg_mnt 
,precio_vta_neto_unit_vig_mnt 
,tipo_precio_vta_cd 
,oferta_cd 
,codigo_de_barras 
,pvp_ant 
,canal_difusion_cd 
,clase_precio_vta_cd
,fecha_inicio 
,centro_cd
) 
Select Distinct 
Tab.articulo_cd
,Tab.umedida_vta_cd
,Tab.precio_vta_unit_reg_mnt
,Tab.iva_precio_vta_unit_reg_mnt
,Tab.imp_especifico_unit_reg_mnt
,Tab.precio_vta_unit_vig_mnt
,Tab.iva_precio_vta_unit_vig_mnt
,Tab.imp_especifico_unit_vig_mnt
,Tab.precio_vta_neto_unit_reg_mnt
,Tab.precio_vta_neto_unit_vig_mnt
,Tab.tipo_precio_vta_cd
,Tab.oferta_cd
,Tab.codigo_de_barras
,Tab.pvp_ant
,Tab.canal_difusion_cd
,Tab.clase_precio_vta_cd
,Tab.fecha_inicio
,Tab.centro_cd 
From (Select 
 Cast(Substr(ITEM.Item_SKU_num,1,18) as varchar(18)) as articulo_cd
,Cast(Substr(ITEM.UOM_conversion_Factor_Qty,1,10) as varchar(10)) as umedida_vta_cd
,Cast(Case
      When PROMO_OFFER_ITEM_PRICE.item_price_amt in('',null) Then '0'
	  Else PROMO_OFFER_ITEM_PRICE.item_price_amt
	  End as Double) as precio_vta_unit_reg_mnt
,0 as iva_precio_vta_unit_reg_mnt
,0 as imp_especifico_unit_reg_mnt
,Cast(Case
      when PROMO_OFFER_ITEM_PRICE.item_price_amt in('',null) Then '0'
	  Else PROMO_OFFER_ITEM_PRICE.item_price_amt
	  End as Double) as precio_vta_unit_vig_mnt
,0 as iva_precio_vta_unit_vig_mnt
,0 as imp_especifico_unit_vig_mnt
,Cast(Case
      when PROMO_OFFER_ITEM_PRICE.item_price_amt in('',null) Then '0'
      else PROMO_OFFER_ITEM_PRICE.item_price_amt
	  End as Double) as precio_vta_neto_unit_reg_mnt
,Cast(Case
      When PROMO_OFFER_ITEM_PRICE.item_price_amt in('',null) Then '0'
	  Else PROMO_OFFER_ITEM_PRICE.item_price_amt
	  End as Double) as precio_vta_neto_unit_vig_mnt
,Cast(Case 
		 When PROMO_OFFER.promo_offer_subtype_cd in('1','2','3','4','5','6','10','11','12','13','14','15') Then 'P'
         When PROMO_OFFER.promo_offer_subtype_cd in('7') Then 'C'
		 When PROMO_OFFER.promo_offer_subtype_cd in('8','9') Then 'D'
    	 Else PROMO_OFFER.promo_offer_subtype_cd
         End as varchar(10)) as tipo_precio_vta_cd 
,Cast(PROMO_OFFER.promo_offer_host_cd as varchar(18)) as oferta_cd
,Cast(ITEM.item_EAN_num as varchar(18)) as codigo_de_barras
,Cast(Case 
      When PROMO_OFFER_ITEM_PRICE.Price_Change_Amt in('',null) Then '0'
      Else PROMO_OFFER_ITEM_PRICE.Price_Change_Amt
	  End as Double) as pvp_ant
,Cast(PROMO_OFFER.marketing_channel_cd as varchar(10)) as canal_difusion_cd
,Cast(PROMO_OFFER.promo_offer_subtype_cd as varchar(10)) as clase_precio_vta_cd
,Cast(PROMO_OFFER.promo_offer_start_dt as varchar(10)) as fecha_inicio
,Cast(LOCATION.Location_host_cd as varchar(10)) as centro_cd
FROM cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER_ITEM_PRICE
Left join cencosud_desa1_datalake_sm_col_teradata.LOCATION on(PROMO_OFFER_ITEM_PRICE.Location_Id = LOCATION.location_id)
Left Join cencosud_desa1_datalake_sm_col_teradata.ITEM on(PROMO_OFFER_ITEM_PRICE.Item_Id = ITEM.Item_id)
Left Join cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER  on(PROMO_OFFER.promo_offer_id = PROMO_OFFER_ITEM_PRICE.promo_offer_id)
WHERE Cast(PROMO_OFFER.promo_offer_start_dt as varchar(10)) Between '${BEGIN_DATE}' and '${END_DATE}') Tab;

exit;
EOF

## log
echo "precio_precio - ${BEGIN_DATE}" >> precio_precio.log

exit $?
