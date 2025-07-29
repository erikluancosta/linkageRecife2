library(DBI)
library(RPostgres)
library(tidyverse)
library(vitallinkage)
library(vitaltable)

# Conexão com o banco de dados
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


# Carregar os dados do SINAN
sinan_viol <- dbGetQuery(con, "SELECT * FROM original_sinan_viol")

# Tabela de padronização
namestand <- vitallinkage::namestand |> 
  mutate(
    fonte =
      case_when(
        fonte=="SINAN"~"SINAN_VIOL",
        TRUE~fonte
      )
  ) |> 
  bind_rows(
    data.frame(
      fonte = c("SINAN_VIOL","SINAN_VIOL", "SINAN_VIOL"),
      var_names_orig = c("id_sinan_viol", "id_registro_linkage","id_unico"),
      stanard_name = c("id_sinan_viol", "id_registro_linkage","id_unico")
    )
  )


# Função unificada para ajustar os textos
ajuste_txt2 <- function(df) {
  
  ## 1. Vetores de apoio ----
  # valores que viram NA
  na_vals   <- c("IGNORADO", "IGNORADA","NAO DECLARADO", "DESCONHECIDO",
                 "NAO INFORMADO","NAO INFORMADA", "NA", "ND", "NAO CONSTA",
                 "IGN", "NC", "XXXXX", "-----", "NAO IDENTIFICADO",
                 "NAO IDENTIFICADA", "NAO REGISTRADO", "NAO REGISTRADA",
                 "NAO DIVULGADO", "N I", "SEM INFORMACOES", "NAO SOUBE INFORMAR")
  
  # palavras-lixo típicas de recém-nascido / natimorto
  lixo_rn   <- "\\b(RN|RECEM NASCIDO|RECEM|NATIMORTO|NATIMORTE|FETO MORTO|FETO|NASCIDO VIVO|NASCIDO)\\b"
  # sufixos de parentesco/ordem
  sufixos   <- "\\b(FILHO|FILHA|NETO|NETA|SOBRINHO|SOBRINHA|JUNIOR|JR|SEGUNDO|TERCEIRO)\\b"
  # conectivos a remover
  conectivos<- "\\b(D[AOE]?|DOS|DAS|DDA|DAA|DO|DOO|DDO|DOOS|DDOS|DOSS|DOS|DE|DDE|DEE|E|MC|DI|DDI|DII|D’)\\b"
  
  ## 2. Pipeline ----
  df %>% 
    # 2a) Padroniza strings que devem virar NA em TODAS as colunas de texto
    mutate(
      across(
        where(is.character),
        \(x) {
          # padroniza para comparar (maiúsculas + sem acento)
          x_clean <- stringi::stri_trans_general(toupper(x), "Latin-ASCII")
          dplyr::case_when(
            x_clean %in% na_vals ~ NA_character_,
            TRUE                 ~ x
          )
        }
      )
    ) %>% 
    # 2b) Limpa apenas as colunas cujo nome contém "_nome"
    mutate(
      across(
        where(is.character) & matches("_nome", ignore.case = TRUE),
        \(x) {
          x %>% 
            toupper() |>                                    # caixa alta
            stringi::stri_trans_general("Latin-ASCII") |>   # remove acento
            str_replace_all("\\s+", " ") |>                 # espaços múltiplos
            str_trim() |> 
            str_remove_all(lixo_rn) |>                      # remove palavras-lixo
            str_remove_all(sufixos) |>                      # remove sufixos
            str_replace_all(conectivos, " ") |>             # remove conectivos
            str_replace_all("[^A-Z ]", " ") |>              # só letras/espaço
            str_replace_all("(.)\\1+", "\\1") |>            # letras repetidas
            str_squish() |>                                 # espaço único
            na_if("")                                       # vazio -> NA
        }
      )
    )
}


# Normalizando os nomes das variáveis
sinan_viol <- sinan_viol |>
  vitallinkage::drop_duplicados_sinan_1() |>  # Dropa as colunas duplicadas inicialmente
  vitallinkage::padroniza_variaveis(namestand,"SINAN_VIOL") |> 
  vitallinkage::ajuste_data(tipo_data=2) |> 
  vitallinkage::ano_sinan() |> 
  mutate(recem_nasc = ifelse(
    grepl("^(RN |RECEM NASCIDO|RN NASCIDO|NATIMORTO|NATIMORTI|FETO MORTO|FETO|MORTO|NASCIDO VIVO|VIVO|NASCIDO|SEM DOC|CADAVER|NATIMORTE|RECEM|IGNORADO|RECEM NASCIDO DE )", ds_nome_pac), 
    1, 
    NA
  )) |> 
  vitallinkage::copia_nomes() |> 
  # Ajusta as variáveis que contem "ds_nome" na composição
  ajuste_txt2() |> 
  vitallinkage::soundex_linkage("ds_nome_pac") |>
  vitallinkage::soundex_linkage("ds_nome_mae") |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "0000", NA, .))) |> 
  # Ajusta as variáveis que contem "_res" na composição
  vitallinkage::ajuste_res() |> 
  vitallinkage::soundex_linkage("ds_bairro_res") |>
  vitallinkage::soundex_linkage("ds_rua_res") |>
  vitallinkage::soundex_linkage("ds_comple_res") |>
  vitallinkage::soundex_linkage("ds_ref_res") |> 
  mutate(across(ends_with("_res2"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(ends_with("_res2_sound"), ~ ifelse(. == "0000", NA, .))) |> 
  # NOVAS VARIÁVEIS
    # Raça/cor
  vitallinkage::ds_raca_sinan() |> 
  mutate(
    ds_raca = stringr::str_to_title(ds_raca),
    ds_raca = case_when(
      ds_raca == "Ignorado" ~ "Ignorada",
      ds_raca == "Indigena" ~ "Indígena",
      TRUE ~ ds_raca)
  ) |> 
    # Sexo
  vitallinkage::corrige_sg_sexo() |> # Corrige os registros de sexo Ignorado
  vitallinkage::nu_idade_anos_sinan() |>  # Discutir a ideia de calcular com a idade antes do código
  select(id_sinan_viol, id_registro_linkage, id_unico, everything()) |>
  arrange(id_sinan_viol)

# função auxiliar: tenta converter para inteiro só se for seguro
safe_as_int <- function(x) {
  # já é inteiro → devolve
  if (is.integer(x)) return(x)
  
  # se for numérico (double), só muda a classe
  if (is.numeric(x)) return(as.integer(x))
  
  # se for character, verifica se todos os valores (não-NA) são dígitos puros
  all_digits <- str_detect(x, "^\\d+$")          # TRUE onde só tem 0-9
  if (all(is.na(x) | all_digits)) {
    return(as.integer(x))                        # seguro → converte
  } else {
    return(x)                                    # mantém como está
  }
}

sinan_viol <- sinan_viol |>  
  mutate(
    across(
      .cols = matches("^(id_|cd_|def_|tran_|rel_|enc_|proc_|viol_)"),   # pega id_* e cd_*
      .fns  = safe_as_int              # aplica regra “tenta, mas só se der”
    )
  )


#-----------------------------------
# Adicionar ao banco de dados
#-----------------------------------

# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("tratado_sinan_viol"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sinan_viol[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sinan_viol = "BIGINT",
                  id_registro_linkage = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE tratado_sinan_viol 
    ADD PRIMARY KEY (id_sinan_viol)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(con, "tratado_sinan_viol", sinan_viol)
tictoc::toc()

