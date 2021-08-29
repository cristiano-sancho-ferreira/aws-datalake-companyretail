Insert into cencosud_desa1_datalake_sm_col_migracion.niv_niv4
		   (operacion_cd 
		   ,secuencial 
		   ,merc_nivel4_cd 
		   ,fecha_novedad 
		   ,merc_nivel4_desc 
		   ,merc_nivel3_cd) 
Select Distinct 
	   Tab.operacion_cd 
	   ,Tab.secuencial 
	   ,Tab.merc_nivel4_cd 
	   ,Tab.fecha_novedad 
	   ,Tab.merc_nivel4_desc 
	   ,Tab.merc_nivel3_cd 
From (Select  'M' as operacion_cd
			  ,'0' as secuencial
			  ,Cast(Substr(item_subclass.item_class_cd,1,16) as varchar(16)) as merc_nivel4_cd 
			  ,'1900-01-01' as fecha_novedad  
			  ,Cast(item_subclass.Item_Subclass_Name as varchar(100)) as merc_nivel4_desc 
			  ,Cast(item_subclass.Item_Class_Cd as varchar(16)) as merc_nivel3_cd  
		From  cencosud_desa1_datalake_sm_col_teradata.item_subclass) Tab
Where Not Exists ( Select 1 from cencosud_desa1_datalake_sm_col.niv_niv4 B
					   Where B.operacion_cd     = Tab.operacion_cd 
						 And B.secuencial       = Tab.secuencial
						 And B.merc_nivel4_cd   = Tab.merc_nivel4_cd
						 And B.merc_nivel4_desc = Tab.merc_nivel4_desc 
						 And B.merc_nivel3_cd   = Tab.merc_nivel3_cd)      
