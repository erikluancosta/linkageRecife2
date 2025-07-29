library(vitallinkage)
library(vitaltable)
library(tidyverse)
library(foreign)

tictoc::tic()

padroniza_data <- function(data_col) {
  suppressWarnings({
    # Identifica os valores numéricos, incluindo negativos e zeros
    numeric_dates <- grepl("^[-]?[0-9]+$", data_col)
    
    # Converte os valores numéricos para datas
    data_col[numeric_dates] <- as.character(as.Date(as.numeric(data_col[numeric_dates]), origin = "1899-12-30"))
    
    # Converte strings no formato 'yyyy-mm-dd' para data
    string_dates <- !numeric_dates
    data_col[string_dates] <- as.character(as.Date(data_col[string_dates], format = "%Y-%m-%d"))
    
    # Mantém os valores que não puderam ser convertidos
    data_col[is.na(data_col)] <- NA
  })
  return(data_col)
}


# SINAN antigo
sinan_a <- read.dbf("1_base_bruta/dados/2023/violencia_2010_2023.dbf")

# Removendo os anos da nova base e padronizando os dados com o pacote vitallinkage
sinan_a <- sinan_a |>
  filter(!NU_ANO %in% c("2022", "2023"))

# SINAN 2022 a 2024
sinan_b <- readxl::read_excel("1_base_bruta/dados/recife_novo_linkage_2024/sinan_viol_2022_2023/VIOLENCIA 2022_2023.xlsx",
                              col_types = "text") |> 
  mutate(across(starts_with("DT_"), padroniza_data))


# SINAN 2022 a 2024
sinan_viol <- bind_rows(sinan_a |> vitallinkage::as_char(), sinan_b |> vitallinkage::as_char())

a <- sinan_viol |> select(starts_with("DT"))

# Criando as colunas de id_unico e id_SINAN_VIOL para identificação dos registros
sinan_viol <- sinan_viol |>
  mutate(
    id_unico = paste0("SINAN_VIOL_", row_number(), "_", NU_NOTIFIC),
    banco = "SINAN_VIOL",
    id_sinan_viol = row_number()
  ) |> 
  select(id_sinan_viol, id_registro_linkage, id_unico, everything())

# Lista de colunas a serem convertidas
cols_to_convert <- c("DS_OBS", "NM_PACIENT", "NM_MAE_PAC", "REL_ESPEC", "NM_LOGRADO", "DS_COMPL",
                     "LESAO_ESPE","VIOL_ESPEC", "LOCAL_ESPE","NO_LOG_OCO", "NM_BA_OCOR",
                     "DS_REF_OCO", "DS_REF_RES", "AG_ESPEC", "DEF_ESPEC", "CONS_ESPEC",
                     "ENC_ESPEC", "DS_COMP_OC")

# Aplicar a conversão em todas as colunas especificadas
sinan_viol[cols_to_convert] <- lapply(sinan_viol[cols_to_convert], function(x) {
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
  name      = SQL("original_sinan_viol"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sinan_viol[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sinan_viol = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE original_sinan_viol 
    ADD PRIMARY KEY (id_sinan_viol)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
#tictoc::tic()
dbAppendTable(con, "original_sinan_viol", sinan_viol)
tictoc::toc()


