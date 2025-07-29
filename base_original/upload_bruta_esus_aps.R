library(vitallinkage)
library(vitaltable)
library(tidyverse)
library(foreign)

load("1_base_bruta/dados/2023/tb_text_pront_atend_202308151906.Rdata")

esus_aps <- esus_ab
rm(esus_ab)


#------------------------------------
# OUTRA ALTERNATIVA DE CARREGAMENTO
#------------------------------------
con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("DB_HOST_AWS"),
  port = as.integer(Sys.getenv(("DB_PORT_AWS"))),
  user = Sys.getenv("DB_USER_AWS"),
  password = Sys.getenv("DB_PASSWORD_AWS"),
  dbname = "esus_aps_completo"
)

# Carregar os dados do banco de dados
esus_aps <- dbGetQuery(con, "SELECT * FROM view_linkage_export")

esus_aps <- esus_aps |> 
  mutate(
    id_unico = paste0("ESUS_APS_", row_number(), "_", tba_co_unico_atend),
    banco = "ESUS_APS",
    id_esus_aps = row_number(),
    id_registro_linkage = -1, # será ajustada depois via script
  ) |> 
  select(id_esus_aps, id_registro_linkage, id_unico, everything())




#-----------------------------------
# Conexão com o banco de dados
#-----------------------------------
source('R/conectar.R')

connn <- conectar('linkage2')


conn <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("DB_HOST_AWS"),
  port = as.integer(Sys.getenv(("DB_PORT_AWS"))),
  user = Sys.getenv("DB_USER_AWS"),
  password = Sys.getenv("DB_PASSWORD_AWS"),
  dbname = "linkage_recife2"
)

# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = connn,
  name      = SQL("original_esus_aps_view"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = esus_aps[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_esus_aps = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(connn, "
  ALTER TABLE original_esus_aps_view
    ADD PRIMARY KEY (id_esus_aps)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(connn, "original_esus_aps_view", esus_aps)
tictoc::toc()