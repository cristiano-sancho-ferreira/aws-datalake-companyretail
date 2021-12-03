# Datalake automatizado
## Executar carga de Datalake com Glue

# Exemplo projeto COLOMBIA

Passo 1: Após a criação do ambiente na AWS, execute o comando abaixo:

cd code
python lambda_function.py "{'database_name':'companyretail_datalake_dev_raw','sql_files_directory':'.\\tabelas','athena_query_output':'s3://aws-athena-resultquery/'}"

Passo 2: Execute o Job 