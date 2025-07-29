library(vitallinkage)
library(vitaltable)
library(tidyverse)
library(foreign)

# SIM antigo
sim_a <- read.dbf("1_base_bruta/dados/2023/DO_Recife_2010_2023.dbf")

sim_a <- sim_a |> 
  mutate(ANO = str_sub(DTOBITO, -4, -1)) |> 
  filter(!ANO %in% c("2022", "2023"))



# SIM novo
sim_b <- read_excel(
  "1_base_bruta/dados/recife_novo_linkage_2024/sim_2022_2023/SIM 2022 a 2023.xlsx",
  col_types = "text"
)

# Criando variável de ano
sim_b <- sim_b |> 
  mutate(ANO = str_sub(DTOBITO, -4, -1))


# Merge
sim <- bind_rows(sim_a, sim_b)

sim <- sim |> mutate(
  id_unico = paste0("SIM_", row_number(), "_", NUMERODO),
  banco = "SIM",
  #id_registro_linkage = -1, # REMOVER
  id_sim = row_number()
) |> 
  select(id_sim, id_registro_linkage, id_unico, everything())


# Lista de colunas a serem convertidas
cols_to_convert <- c("ENDRES", "NOME", "NOMEPAI", "NOMEMAE", "COMPLRES", 
                     "ENDOCOR", "COMPLOCOR", "DSEVENTO", "ENDACID")

# Aplicar a conversão em todas as colunas especificadas
sim[cols_to_convert] <- lapply(sim[cols_to_convert], function(x) {
  iconv(x, from = "latin1", to = "UTF-8", sub = "byte")
})


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
  name      = SQL("original_sim"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sim[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sim= "BIGINT")
)
# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE original_sim 
    ADD PRIMARY KEY (id_sim)
")

# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(con, "original_sim", sim)
tictoc::toc()
