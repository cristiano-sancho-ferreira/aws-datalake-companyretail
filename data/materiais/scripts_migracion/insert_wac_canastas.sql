Insert into cencosud_desa1_datalake_sm_col_migracion.wac_canastas
(negocioid 
,ean 
,sku 
,codigotipocanasta 
,codigodescuento
,fechacanasta
,tiendaid)

Select Distinct Tab.negocioid
				,Tab.ean 
				,Tab.sku  
				,Tab.codigotipocanasta  
				,Tab.codigodescuento
				,Tab.fechacanasta
				,Tab.tiendaid
From(Select Distinct
      Cast(ITEM.chain_cd as Integer) as negocioid
      ,Cast(Case
      when ITEM.item_EAN_num in('',null) then '0'
      Else ITEM.item_EAN_num
      End as Double) as ean
     ,Cast(Substr(ITEM.item_SKU_num,1,15) as varchar(15)) as sku
     ,Cast(LOCATION_ITEM_GROUP.Location_Item_Group_Type_Cd as varchar(03)) as codigotipocanasta
	 ,Cast(Case
          When PROMO_OFFER.Promo_Offer_host_cd in('',null) then '0'
          Else PROMO_OFFER.Promo_Offer_host_cd 
          End as double) as codigodescuento 
     ,Cast(LOCATION_ITEM_GROUP.Location_Item_Group_Start_Dt as varchar(10)) as fechacanasta
	 ,Cast(LOCATION.Location_Host_cd as varchar(50)) as tiendaid
     From cencosud_desa1_datalake_sm_col_teradata.LOCATION_ITEM_GROUP
     Left join cencosud_desa1_datalake_sm_col_teradata.LOCATION  on (LOCATION_ITEM_GROUP.Location_Id = LOCATION.Location_id )
     Left Join cencosud_desa1_datalake_sm_col_teradata.ITEM on(LOCATION_ITEM_GROUP.Item_Id = ITEM.item_id)
     Left join cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER on(LOCATION_ITEM_GROUP.Promo_Offer_Id = PROMO_OFFER.promo_offer_id)) Tab