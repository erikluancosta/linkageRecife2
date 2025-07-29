library(vitallinkage)
library(vitaltable)
library(tidyverse)
library(foreign)
library(tictoc)
library(data.table)

# Tratamento SIH
sih <- read.csv2("1_base_bruta/dados/2023/TB_HAIH.CSV",
                 sep="," ,
                 colClasses = c("AH_PACIENTE_NUMERO_CNS" = "character",
                                "AH_NUM_AIH" = "character"),
                 encoding = 'latin1')

sih <- sih |> 
  mutate(id_unico = paste0("SIH_", row_number(), "_", AH_NUM_AIH),
         banco = "SIH",
         id_registro_linkage = -1, # será ajustada depois via script
         id_sih = row_number()
         ) |> 
  select(id_sih, id_registro_linkage, id_unico, everything())



#-----------------------------------
# Conexão com o banco de dados
#-----------------------------------
source('R/conectar.R')

con <- conectar('linkage2')

# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("original_sih"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sih[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sih = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE original_sih 
    ADD PRIMARY KEY (id_sih)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(con, "original_sih", sih)
tictoc::toc()