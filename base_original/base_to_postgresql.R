# Carregar o pacote
library(RPostgres)

# Configurar a conexão ao banco de dados PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = 5432,          # Porta padrão do PostgreSQL
  user = "postgres",
  password = "123",
  dbname = "linkage_recife"
)

# Verificar se a conexão foi estabelecida com sucesso
if (dbIsValid(con)) {
  print("Conexão estabelecida com sucesso!")
} else {
  print("Falha na conexão ao banco de dados.")
}

# Para desconectar quando terminar de usar o banco de dados
# dbDisconnect(con)


# Escrever o dataframe df no banco de dados como uma tabela chamada "tratado_sih"
dbWriteTable(con, "temp_sinasc_linkage", sinasc_linkage, overwrite = TRUE, row.names = FALSE)


# Verificar se a tabela foi criada com sucesso
if ("linkage_temp2" %in% dbListTables(con)) {
  print("Tabela 'linkage_temp2' criada com sucesso!")
} else {
  print("Falha na criação da tabela 'linkage_temp2'.")
}

