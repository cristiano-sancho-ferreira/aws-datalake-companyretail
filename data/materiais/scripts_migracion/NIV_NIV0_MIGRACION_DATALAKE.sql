Insert into cencosud_desa1_datalake_sm_col_migracion.niv_niv0
		(operacion_cd 
		 ,secuencial
		 ,merc_nivel0_cd
		 ,fecha_novedad 
		 ,merc_nivel0_desc) 
Select  Tab.operacion_cd 
		 ,Tab.secuencial
		 ,Tab.merc_nivel0_cd
		 ,Tab.fecha_novedad 
		 ,Tab.merc_nivel0_desc
From (select  'M' as operacion_cd
        ,'0' as secuencial
        ,Cast(comercial_group.group_cd as varchar(16)) as merc_nivel0_cd
        ,'1900-01-01' as fecha_novedad  
        ,Cast(comercial_group.group_name as varchar(100)) as merc_nivel0_desc
 From  cencosud_desa1_datalake_sm_col_teradata.comercial_group) Tab
 Where Not Exists ( Select 1 from cencosud_desa1_datalake_sm_col.niv_niv0 B   
                      Where B.operacion_cd     = Tab.operacion_cd   
						And B.secuencial       = Tab.secuencial 
						And B.merc_nivel0_cd   = Tab.merc_nivel0_cd
						And	B.fecha_novedad    = Tab.fecha_novedad 
						And	B.merc_nivel0_desc = Tab.merc_nivel0_desc)
                       