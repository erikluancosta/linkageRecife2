library(foreign)
library(tidyverse)
library(readxl)
library(vitaltable)
library(read.dbc)
library(vitallinkage)



# Carregando a base de intoxicação exogena
sinan_iexo <- readxl::read_excel("1_base_bruta/dados/recife_novo_linkage_2024/sinan_intox_2010_2023/INTOXICACAO 2010_2023.xlsx",
                                 col_types = "text") |> 
  mutate(across(starts_with("DT"), ~ as.Date("1899-12-30") + as.numeric(.)))

# Ajustar as datas para o formato nao numerico

sinan_iexo <- sinan_iexo |> 
  mutate(
    id_unico = paste0("SINAN_IEXO_", row_number(), "_", NU_NOTIFIC),
    id_registro_linkage = -1, # será ajustada depois via script
    banco = "SINAN_IEXO",
    id_sinan_iexo = row_number()
  ) |> 
  select(id_sinan_iexo, id_registro_linkage, id_unico, everything())





#-----------------------------------
# Conexão com o banco de dados
#-----------------------------------
library(RPostgres)
library(DBI)

# Configurar a conexão ao banco de dados PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = 5432,          # Porta padrão do PostgreSQL
  user = "postgres",
  password = "123",
  dbname = "linkage_recife_novo"
)

# Verificar se a conexão foi estabelecida com sucesso
if (dbIsValid(con)) {
  print("Conexão estabelecida com sucesso!")
} else {
  print("Falha na conexão ao banco de dados.")
}


# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("original_sinan_iexo"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sinan_iexo[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sinan_iexo = "BIGINT")
  
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------

dbExecute(con, "
  ALTER TABLE original_sinan_iexo 
    ADD PRIMARY KEY (id_sinan_iexo)
")

# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(con, "original_sinan_iexo", sinan_iexo)
tictoc::toc()





# Na tratada criar as colunas para referenciar o ID do original e uma que referencia o id_registro_linkage. não NULL
# Tratada referencia a original na coluna id_sinan_iexo

# Proprio id_tratado_sinan_iexo - marca a própria chave


# TABELA: tratado_sinan_iexo

# PK: id_sinan_iexo (FK: original_sinan_iexo.id_sinan_iexo)
# FK: id_registro_linkage (merge com registro_linkage via id_unico)

# TABELA: original_sinan_iexo

# PK: id_sinan_iexo (1 a N)






















