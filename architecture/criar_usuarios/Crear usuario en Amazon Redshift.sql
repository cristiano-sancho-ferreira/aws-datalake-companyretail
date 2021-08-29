----------Script de creación de usuario en Amazon Redshift----------

--Creaar usuario
create user nome_usuario password 'senha_usuario';

--Permiso para ejecutar select en un schema específico
grant select on all tables in schema col_super_dim_tb to cristian_nunez;
grant select on all tables in schema col_super_dim_vw to cristian_nunez;
grant select on all tables in schema col_super_rel_tb to cristian_nunez;
grant select on all tables in schema col_super_rel_vw to cristian_nunez;
grant select on all tables in schema col_super_stg to cristian_nunez;
grant select on all tables in schema cuadraturas_dim_tb to cristian_nunez;
grant select on all tables in schema dataload to cristian_nunez;
grant select on all tables in schema public to cristian_nunez;

--Remover permiso total de usario 
revoke all on schema nome_schema from nome_usuario