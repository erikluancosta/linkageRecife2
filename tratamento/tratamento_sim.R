library(vitallinkage)
library(vitaltable)
library(tidyverse)
library(foreign)
library(RPostgres)
library(DBI)
tictoc::tic()
# Configurar a conexão ao banco de dados PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  host = "localhost",
  port = 5432,          # Porta padrão do PostgreSQL
  user = "postgres",
  password = "123",
  dbname = "linkage_recife_novo"
)

# Carregar os dados do banco de dados
sim <- dbGetQuery(con, "SELECT * FROM original_sim")

namestand2 <- vitallinkage::namestand |> 
  bind_rows(
    data.frame(
      fonte = c("SIM","SIM", "SIM", "SIM"),
      var_names_orig = c("id_sim", "id_registro_linkage","id_unico", "DTATESTADO"),
      stanard_name = c("id_sim", "id_registro_linkage","id_unico", "dt_atestado")
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
  lixo_rn   <- "\\b(RN |FM |FM1 |FM2 |FMI |FMII |IFM|RECEM NASCIDO|RN NASCIDO|NATIMORTO|NATIMORTI|FETO MORTO|FETO|MORTO|NASCIDO VIVO|VIVO|NASCIDO|SEM DOC|CADAVER|NATIMORTE|RECEM|IGNORADO|RECEM NASCIDO DE )\\b"
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


sim2 <- sim |> 
  vitallinkage::upper_case_char() |>
  vitallinkage::padroniza_variaveis(namestand2,nome_base = "SIM") |> 
  vitallinkage::ajuste_data(tipo_data=1) |>
  vitallinkage::ano_sim() |> # Adicionando o ano
  vitallinkage::copia_nomes() |>
  vitallinkage::gemelar("ds_nome_pac") |> # Cria coluna de gemelar
  mutate(
    recem_nasc = ifelse(
      grepl(
        "^(RN |FM |FM1 |FM2 |FMI |FMII |IFM|RECEM NASCIDO|RN NASCIDO|NATIMORTO|NATIMORTI|FETO MORTO|FETO|MORTO|NASCIDO VIVO|VIVO|NASCIDO|SEM DOC|CADAVER|NATIMORTE|RECEM|IGNORADO|RECEM NASCIDO DE )", 
        ds_nome_pac), 1, NA),
    nu_cns = str_trim(case_when(
      nu_cns == "000000000000000" ~ NA_character_,
      nu_cns == "00000000000" ~ NA_character_,
      nu_cns == "000000000" ~ NA_character_,
      nu_cns == "00000000000000" ~ NA_character_,
      nu_cns == "000000000000000" ~ NA_character_,
      nu_cns == "000000000000001" ~ NA_character_,
      nu_cns == "000000000000002" ~ NA_character_,
      nu_cns == "000000000000003" ~ NA_character_,
      nu_cns == "000000000000004" ~ NA_character_,
      nu_cns == "000000000000005" ~ NA_character_,
      nu_cns == "000000000000006" ~ NA_character_,
      nu_cns == "000000000000007" ~ NA_character_,
      nu_cns == "000000000000008" ~ NA_character_,
      nu_cns == "000000000000009" ~ NA_character_,
      nu_cns == "000000000000010" ~ NA_character_,
      nu_cns == "000000000000011" ~ NA_character_,
      nu_cns == "" ~ NA_character_,
      TRUE ~ nu_cns
    ))
  )|> 
  ajuste_txt2() |> 
  vitallinkage::soundex_linkage("ds_nome_pac") |>
  vitallinkage::soundex_linkage("ds_nome_pai") |>
  vitallinkage::soundex_linkage("ds_nome_mae") |>
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "0000", NA, .))) |> 
  vitallinkage::ajuste_res() |> # Ajusta as variáveis que contem "_res" na composição
  vitallinkage::soundex_linkage("ds_bairro_res") |>
  vitallinkage::soundex_linkage("ds_rua_res") |>
  vitallinkage::soundex_linkage("ds_comple_res") |>
  mutate(across(ends_with("_res2"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(ends_with("_res2_sound"), ~ ifelse(. == "0000", NA, .))) |> 
  vitallinkage::drop_duplicados_sim_padronizado() |> 
  ## NOVAS VARIÁVEIS
  vitallinkage::ds_raca_sim() |> # Ajustando a raça/cor
  vitallinkage::corrige_sg_sexo() |> # Ajustando a variável sg_sexo
  vitallinkage::nu_idade_anos_sim() |> # Ajustanso a idade em anos
  dplyr::mutate(morreu=1) |>  
  mutate(
    ds_raca = stringr::str_to_title(ds_raca),
    ds_raca = case_when(
      ds_raca == "Ignorado" ~ "Ignorada",
      ds_raca == "Indigena" ~ "Indígena",
      TRUE ~ ds_raca)
  ) |> 
  mutate(
    ano = as.numeric(ano),
    ano_nasc = year(dt_nasc)
  ) |> 
  select(id_sim, id_registro_linkage, id_unico, everything()) |>
  arrange(id_sim)


sim <- sim2
rm(sim2)
#-----------------------------------
# Adicionar ao banco de dados
#-----------------------------------

# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("tratado_sim"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sim[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sim = "BIGINT",
                  id_registro_linkage = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE tratado_sim 
    ADD PRIMARY KEY (id_sim)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
#tictoc::tic()
dbAppendTable(con, "tratado_sim", sim)
tictoc::toc()


