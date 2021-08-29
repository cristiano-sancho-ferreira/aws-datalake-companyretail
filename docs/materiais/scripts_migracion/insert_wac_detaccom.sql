Insert into cencosud_desa1_datalake_sm_col_migracion.wac_detaccom
(codigodescuento 
,tiendaid 
,ean 
,sku 
,numeropagina 
,previstoventabruta 
,previstoventaneta 
,previstounidades 
,previstomargenpesos 
,negocioid)
Select Distinct codigodescuento 
	,Tab.tiendaid 
	,Tab.ean 
	,Tab.sku 
	,Tab.numeropagina 
	,Tab.previstoventabruta 
	,Tab.previstoventaneta 
	,Tab.previstounidades 
	,Tab.previstomargenpesos 
	,Tab.negocioid
From( Select  
       Cast(Case
            When PROMO_OFFER.promo_offer_host_cd in('',null) Then '0'
            Else PROMO_OFFER.promo_offer_host_cd 
            End as double) as CodigoDescuento
	   ,Cast(PROMO_OFFER_LOCATION.Location_Id as varchar(50)) as TiendaId
	   ,Cast(Case
			When  ITEM.item_ean_num in('',null) then '0'
			Else  ITEM.item_ean_num
			End as Double) as EAN
	   ,Cast(ITEM.item_sku_num as varchar(15)) as SKU
	   ,Cast(Case
			When  PROMO_OFFER_XREF.Promo_Offer_Line_Num in('',null) then '0'
			Else  PROMO_OFFER_XREF.Promo_Offer_Line_Num
			End as Integer) as NumeroPagina
		,Cast(Case
			 When  PROMO_OFFER_XREF.Promo_Sales_Goal_Amt  in('',null) then '0'
			 Else  PROMO_OFFER_XREF.Promo_Sales_Goal_Amt 
			 End as Double) as PrevistoVentaBruta
		,Cast(Case
             When  PROMO_OFFER_XREF.Promo_Net_Sales_Goal_Amt  in('',null) then '0'
             Else  PROMO_OFFER_XREF.Promo_Net_Sales_Goal_Amt 
             End as double) as PrevistoVentaNeta
        ,Cast(Case
			 When  PROMO_OFFER_XREF.Promo_Item_Qty   in('',null) then '0'
			 Else  PROMO_OFFER_XREF.Promo_Item_Qty  
			 End as Double) as PrevistoUnidades
		,Cast(Case
			 When  PROMO_OFFER_XREF.Promo_Margin_Amt   in('',null) then '0'
             Else  PROMO_OFFER_XREF.Promo_Margin_Amt 
             End as double) as PrevistoMargenPesos
        ,Cast('1' as varchar(3)) as NegocioId
     FROM cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER 
JOIN cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER_LOCATION 
  ON (PROMO_OFFER_LOCATION.Promo_Offer_Id = PROMO_OFFER.promo_offer_id)
JOIN cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER_XREF
  ON (PROMO_OFFER_XREF.promo_offer_id = PROMO_OFFER.promo_offer_id )
JOIN cencosud_desa1_datalake_sm_col_teradata.ITEM
  ON (PROMO_OFFER_XREF.item_Id = ITEM.item_id)) Tab