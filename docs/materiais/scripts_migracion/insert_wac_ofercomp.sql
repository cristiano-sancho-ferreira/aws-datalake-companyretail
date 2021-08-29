insert into cencosud_desa1_datalake_sm_col_migracion.wac_ofercomp(
CodigoDescuento
,CodigoAccionComercial
,DescripcionDescuento
,CodigoTipoPrecio
,FechaInicioDescuento
,FechaPlaneadaFinDescuento
,FechaRealFinDescuento
,CodigoSistemaFuente
,CodigoMedioDifusion
,NegocioId
,CodigoTipoPromocion
,CodigoClasePromocion)
Select Distinct Tab.codigodescuento
    ,Tab.codigoaccioncomercial 
    ,Tab.descripciondescuento 
    ,Tab.codigotipoprecio 
    ,Tab.fechainiciodescuento 
    ,Tab.fechaplaneadafindescuento 
    ,Tab.fecharealfindescuento 
    ,Tab.codigosistemafuente 
    ,Tab.codigomediodifusion 
    ,Tab.negocioid 
    ,Tab.codigotipopromocion 
    ,Tab.codigoclasepromocion 
From (Select Cast(PROMO_OFFER.Promo_offer_host_cd as varchar(50)) as CodigoDescuento   
        ,Cast(COMMERCIAL_ACTION.commercial_action_host_cd as varchar(20)) as CodigoAccionComercial
        ,Cast(PROMO_OFFER.Promo_Offer_Desc as varchar(50)) as DescripcionDescuento
        ,Cast(Case 
              When PROMO_OFFER.promo_offer_subtype_cd in('',null) Then Null 
              Else PROMO_OFFER.promo_offer_subtype_cd
              End as Integer) as CodigoTipoPrecio
        ,Cast((PROMO_OFFER.promo_offer_start_dt || PROMO_OFFER.promo_offer_start_tm) as varchar(10)) as FechaInicioDescuento                
        ,Cast(PROMO_OFFER.promo_offer_end_dt || PROMO_OFFER.promo_offer_end_tm as varchar(10)) as FechaPlaneadaFinDescuento
        ,Cast((PROMO_OFFER.Real_promo_offer_end_dt || PROMO_OFFER.Real_promo_offer_end_tm) as varchar(10)) as FechaRealFinDescuento
        ,Cast(PROMO_OFFER.boss_application_cd as varchar(03)) as CodigoSistemaFuente
        ,Cast(PROMO_OFFER.marketing_channel_cd as varchar(03)) as CodigoMedioDifusion
        ,Cast(PROMO_OFFER.Chain_Cd as varchar(03)) as NegocioId
        ,Cast(PROMO_OFFER.Promo_Offer_Type_Cd as varchar(20)) as CodigoTipoPromocion
        ,Cast(PROMO_OFFER.Promo_Offer_Class_Cd as varchar(20)) as CodigoClasePromocion
        From cencosud_desa1_datalake_sm_col_teradata.PROMO_OFFER 
        left Join cencosud_desa1_datalake_sm_col_teradata.COMMERCIAL_ACTION  on (PROMO_OFFER.Commercial_Action_Id = COMMERCIAL_ACTION.commercial_action_id)) Tab
