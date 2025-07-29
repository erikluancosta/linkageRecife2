library(vitallinkage)
library(vitaltable)
library(tidyverse)
library(foreign)
library(RPostgres)
library(DBI)

# Configurar a conexão ao banco de dados PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("DB_HOST_LINKAGE2"),
  port = as.integer(Sys.getenv(("DB_PORT_LINKAGE2"))),
  user = Sys.getenv("DB_USER_LINKAGE2"),
  password = Sys.getenv("DB_PASSWORD_LINKAGE2"),
  dbname = Sys.getenv("DB_NAME_LINKAGE2")
)

# Carregar os dados do banco de dados
esus_aps <- dbGetQuery(con, "SELECT * FROM original_esus_aps_view")

namestand2 <- vitallinkage::namestand |> 
  mutate(
    fonte =
      case_when(
        fonte=="ESUS_AB"~"ESUS_APS",
        TRUE~fonte
      )
  ) |> 
  bind_rows(
    data.frame(
      fonte = c("ESUS_APS","ESUS_APS", "ESUS_APS", "ESUS_APS", "ESUS_APS"),
      var_names_orig = c("id_esus_aps", "id_registro_linkage","id_unico", "nu_cnes", "enc_nu_cid10"),
      stanard_name = c("id_esus_aps", "id_registro_linkage","id_unico", "nu_cnes", "enc_nu_cid10")
    )
  )  |> 
  filter(
    !(var_names_orig %in% c("tba_co_unidade_saude", "tbenc_co_cid10", "tbenc_co_ciap"))
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



esus_aps <- esus_aps |> 
  vitallinkage::padroniza_variaveis(namestand2,'ESUS_APS') |> 
  vitallinkage::upper_case_char() |>
  vitallinkage::ajuste_data(tipo_data=2) |> 
  mutate(nu_cns = as.character(nu_cns),
         across(starts_with("ds"), ~ ifelse(. == "", NA, .)),
         recem_nasc = ifelse(
           grepl("^(RN |RECEM NASCIDO|RN NASCIDO|NATIMORTO|NATIMORTI|FETO MORTO|FETO|MORTO|NASCIDO VIVO|VIVO|NASCIDO|SEM DOC|CADAVER|NATIMORTE|RECEM|IGNORADO|RECEM NASCIDO DE )", ds_nome_pac), 
           1, 
           NA
         )) |> 
  ajuste_txt2() |> 
  vitallinkage::soundex_linkage('ds_nome_pac') |> 
  vitallinkage::soundex_linkage('ds_nome_mae') |> 
  vitallinkage::soundex_linkage('ds_nome_pai') |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "0000", NA, .))) |> 
  vitallinkage::ajuste_res() |>
  vitallinkage::soundex_linkage("ds_bairro_res") |> 
  vitallinkage::soundex_linkage("ds_rua_res") |> 
  mutate(across(ends_with("_res2"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(ends_with("_res2_sound"), ~ ifelse(. == "0000", NA, .)))

esus_aps |> vitaltable::tab_1(ds_sexo)

esus_aps <- esus_aps |> 
  mutate(
    ds_sexo = case_when(
      ds_sexo == "MASCULINO" ~ "M",
      ds_sexo == "FEMININO" ~ "F",
      TRUE ~ "I"
    )
#    ds_sexo = factor(ds_sexo, levels = c("M", "F"))
  
  )


esus_aps$recem_nasc

vitaltable::tab_1(esus_aps,ds_sexo)
#-----------------------------------
# Adicionar ao banco de dados
#-----------------------------------

# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("tratado_esus_aps_view"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = esus_aps[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_esus_aps = "BIGINT",
                  id_registro_linkage = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE tratado_esus_aps_view 
    ADD PRIMARY KEY (id_esus_aps)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(con, "tratado_esus_aps_view", esus_aps)
tictoc::toc()
