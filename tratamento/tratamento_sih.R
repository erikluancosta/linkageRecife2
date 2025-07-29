library(DBI)
library(RPostgres)
library(tidyverse)
library(vitallinkage)
library(vitaltable)
source('conectar/conectar.R')
source('funcoes/namestand.R')
con <- conectar("aws")


# Carregar os dados do SINAN
sih <- dbGetQuery(con, "SELECT * FROM original_sih")

# Tabela de padronização
namestand <- vitallinkage::namestand |> 
  bind_rows(
    data.frame(
      fonte = c("SIH","SIH", "SIH"),
      var_names_orig = c("id_sih", "id_unico"),
      stanard_name = c("id_sih", "id_unico")
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
  df |> 
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
    ) |> 
    # 2b) Limpa apenas as colunas cujo nome contém "_nome"
    mutate(
      across(
        where(is.character) & matches("_nome", ignore.case = TRUE),
        \(x) {
          x |> 
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

tictoc::tic()
sih2 <- sih |> 
  vitallinkage::upper_case_char() |> # As colunas com texto passam a ficar com letra maiuscula
  vitallinkage::padroniza_variaveis(namestand,'SIH') |> 
  mutate(
    id_registro_linkage = -1,
    nu_cns = str_trim(case_when(
      nu_cns == "000000000000000" ~ NA_character_,
      nu_cns == "00000000000" ~ NA_character_,
      nu_cns == "000000000" ~ NA_character_,
      nu_cns == "" ~ NA_character_,
      TRUE ~ nu_cns
    )),
    nu_doc = str_trim(case_when(
      nu_doc == "00000000000000000000000000000000" ~ NA_character_,
      nu_doc == "00000000000000000000000000" ~ NA_character_,
      nu_doc == "00000000000" ~ NA_character_,
      nu_doc == "000000000" ~ NA_character_,
      nu_doc == "" ~ NA_character_,
      nu_doc == "                                " ~ NA_character_,
      nu_doc == "NT"~ NA_character_,
      nu_doc == "SEMDOCUMENTO" ~ NA_character_,
      nu_doc == "ESTUDANTE"~ NA_character_,
      TRUE ~ nu_doc
    )),
    across(starts_with("cd_diag_"), ~ ifelse(. == "    ", NA, .)),
    across(starts_with("cd_diag_"), ~ ifelse(. == "0000", NA, .)),
    diag_obito = ifelse(diag_obito == "    ", NA, diag_obito),
    diag_obito = ifelse(diag_obito == "0000", NA, diag_obito)
  ) |> 
  vitallinkage::ajuste_data(tipo_data = 2) |> 
  vitallinkage::copia_nomes() |> 
  mutate(
    across(starts_with("ds_"), str_trim),
    recem_nasc = ifelse(
      grepl("^(RN |RECEM NASCIDO|RN NASCIDO|NATIMORTO|NATIMORTI|FETO MORTO|FETO|MORTO|NASCIDO VIVO|VIVO|NASCIDO|SEM DOC|CADAVER|NATIMORTE|RECEM|RECEM NASCIDO DE )", ds_nome_pac, ignore.case = TRUE), 
      1, 
      NA
    ),
    ano = year(dt_internacao),
    ano_nasc = year(dt_nasc)
  ) |> 
  ajuste_txt2() |> 
  vitallinkage::soundex_linkage("ds_nome_pac") |>
  vitallinkage::soundex_linkage("ds_nome_mae") |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "0000", NA, .))) |> 
  # Ajusta as variáveis que contem "_res" na composição
  vitallinkage::ajuste_res() |> 
  mutate(
    across(ends_with("_res"), str_trim),
    across(ends_with("_res"), ~ ifelse(. == "", NA, .))
  ) |> 
  vitallinkage::soundex_linkage("ds_bairro_res") |>
  vitallinkage::soundex_linkage("ds_comple_res") |>
  vitallinkage::soundex_linkage("ds_nome_pac_res") |>
  vitallinkage::soundex_linkage("ds_rua_res")|> 
  mutate(across(ends_with("_res2"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(ends_with("_res2_sound"), ~ ifelse(. == "0000", NA, .))) |> 
  vitallinkage::ds_raca_sih() |> 
  vitallinkage::corrige_sg_sexo() |>
  vitallinkage::nu_idade_anos_sih() |> 
  mutate(ds_raca = stringr::str_to_title(ds_raca),
         ds_raca = case_when(
           ds_raca == "Ignorado" ~ "Ignorada",
           ds_raca == "Indigena" ~ "Indígena",
           TRUE ~ ds_raca))
tictoc::toc()

sih <- sih2 |> 
  select(id_sih, id_registro_linkage, id_unico, everything()) |> 
  arrange(id_sih)


#-----------------------------------
# Conexão com o banco de dados
#-----------------------------------
library(RPostgres)
library(DBI)


# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("tratado_sih"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sih[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sih = "BIGINT",
                  id_registro_linkage = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE tratado_sih 
    ADD PRIMARY KEY (id_sih)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
tictoc::tic()
dbAppendTable(con, "tratado_sih", sih)
tictoc::toc()
