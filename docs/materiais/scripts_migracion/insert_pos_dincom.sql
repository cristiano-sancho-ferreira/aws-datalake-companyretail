Insert into cencosud_desa1_datalake_sm_col_migracion.pos_dincom(
negocioid,
dinamicacomercialid,
tipodinamicacomercialid,
clasedinamica_comercialid,
dinamicadescripcion,
fecha_inicio_dinamica,
fecha_fin_dinamica,
cdpais,
cdtipodescuento,
cdmodalidad,
cdmediopago,
cdbines,
cdprograma,
cdsubprogrma,
cdtipovalor,
valor,
cantidadesxy,
cdseccion,
cdcodigoevento,
cdnegocio,
cdformato)
Select Distinct Tab.negocioid
,Tab.dinamicacomercialid
,Tab.tipodinamicacomercialid
,Tab.clasedinamica_comercialid
,Tab.dinamicadescripcion
,Tab.fecha_inicio_dinamica
,Tab.fecha_fin_dinamica
,Tab.cdpais
,Tab.cdtipodescuento
,Tab.cdmodalidad
,Tab.cdmediopago
,Tab.cdbines
,Tab.cdprograma
,Tab.cdsubprogrma
,Tab.cdtipovalor
,Tab.valor
,Tab.cantidadesxy
,Tab.cdseccion
,Tab.cdcodigoevento
,Tab.cdnegocio
,Tab.cdformato 
From(Select 
		Cast(1 as varchar(3)) as negocioid,
		Cast(PROMO_OFFER.promo_offer_host_cd as varchar(20)) as dinamicacomercialid,
		Cast(PROMOTION.Promotion_Type_Cd as varchar(3)) as tipodinamicacomercialid,
		Cast(PROMOTION.Promo_Class_Type_Cd as varchar(3)) as clasedinamica_comercialid,
		Cast(PROMO_OFFER.Promo_offer_Desc as varchar(50)) as dinamicadescripcion,
		Cast(PROMO_OFFER.Promo_offer_Start_Dt as varchar(10)) as fecha_inicio_dinamica,
		Cast(PROMO_OFFER.Promo_Offer_End_Dt as varchar(10)) as fecha_fin_dinamica,
		Cast(169 as integer) as cdpais,
		Cast(PROMOTION.Discount_Type_Cd as varchar(5)) as cdtipodescuento,
		Cast(PROMOTION.Promo_Modality_Type_Cd as varchar(5)) as cdmodalidad,
		Cast(PROMOTION.Payment_Type_Id as varchar(5)) as cdmediopago,
		Cast(PROMOTION.BIN_Group_Cd as varchar(5)) as cdbines,
		Cast(PROMOTION.Promo_Program_Cd as varchar(5)) as cdprograma,
		Cast(PROMOTION.Promo_SubProgram_Cd as varchar(15)) as cdsubprogrma,
		Cast(PROMOTION.Discount_Val_Type_Cd as varchar(5)) as cdtipovalor,
		Cast(Case
			  When  PROMOTION.Discount_Amt in('',null) then '0'
			  else  PROMOTION.Discount_Amt
			  end as double) as valor,
		Cast(Case
			  When  PROMOTION.X_Y_Cnt in('',null) then '0'
			  else  PROMOTION.X_Y_Cnt
			  end as integer) as cantidadesxy,
		Cast(PROMOTION.Group_Cd as varchar(5)) as cdseccion,
		Cast(PROMO_OFFER.promo_offer_host_cd as varchar(20)) as cdcodigoevento,
		Cast(PROMOTION.Location_Class_Cd as varchar(5)) as cdnegocio,
		Cast(PROMOTION.Store_Format_Cd as varchar(5)) as cdformato
  From cencosud_desa1_datalake_sm_col_teradata.PROMOTION 
  Join cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER
    On ( PROMOTION.promo_id = PROMO_OFFER.promo_id 
    And  PROMOTION.promo_host_cd = PROMO_OFFER.promo_offer_host_cd)) Tab