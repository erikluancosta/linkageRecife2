library(DBI)
library(RPostgres)
library(tidyverse)
library(vitallinkage)
library(vitaltable)
source('R/conectar.R')

con <- conectar('linkage2')

# Carregar os dados do SINAN
sinan_iexo <- dbGetQuery(con, "SELECT * FROM original_sinan_iexo;")


# Ano de nascimento
sinan_iexo <- sinan_iexo |> 
  mutate(
    ANO_NASC = year(as.Date(DT_NASC, format = "%Y-%m-%d"))
  )


df_clean <- sinan_iexo

# Repetições no número da notificação
df_clean <- df_clean |> 
  start_linkage(
    c("NU_NOTIFIC", "SOUNDEX"), 
    "NU_NOTIFIC"
  )


# Primeiro, defina a ordem para a variável "CS_SEXO"
df_clean$CS_SEXO <- factor(df_clean$CS_SEXO, levels = c("I", "M", "F"), ordered = TRUE)

# Liste as variáveis a serem modificadas, excluindo "par_1" e "CS_SEXO"
vars_to_mutate <- names(df_clean)[!(names(df_clean) %in% c("par_1", "CS_SEXO"))]

# Aplique as transformações usando group_by e mutate
df_clean <- df_clean %>%
  group_by(par_1) %>%
  mutate(
    across(
      all_of(vars_to_mutate),
      ~ if (!is.na(par_1[1])) {
        if (all(is.na(.))) {
          NA
        } else {
          max(., na.rm = TRUE)
        }
      } else {
        .
      }
    ),
    CS_SEXO = if (!is.na(par_1[1])) {
      if (all(is.na(CS_SEXO))) {
        NA
      } else {
        as.character(max(CS_SEXO, na.rm = TRUE))
      }
    } else {
      CS_SEXO
    }
  ) |>
  ungroup() |>
  unique() |> 
  select(
    -par_1,
    -par_c1,
    -regra1
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


## PADRONIZAR OS NOMES DAS VARIÁVEIS
names_sinan_iexo <- readxl::read_xlsx("1_base_bruta/dados/nm_stand_sinan_intox_exogena.xlsx")

df_clean <- df_clean |> 
  vitallinkage::padroniza_variaveis(names_sinan_iexo,nome_base = "SINAN_IEXO") |> 
  vitallinkage::copia_nomes() |> 
  vitallinkage::ano_sinan() |> 
  mutate(recem_nasc = ifelse(
    grepl("^(RN |RECEM NASCIDO|RN NASCIDO|NATIMORTO|NATIMORTI|FETO MORTO|FETO|MORTO|NASCIDO VIVO|VIVO|NASCIDO|SEM DOC|CADAVER|NATIMORTE|RECEM|IGNORADO|RECEM NASCIDO DE )", ds_nome_pac), 
    1, 
    NA
  )) |> 
  # Ajusta as variáveis que contem "ds_nome" na composição
  ajuste_txt2() |> 
  vitallinkage::soundex_linkage("ds_nome_pac") |>
  vitallinkage::soundex_linkage("ds_nome_mae") |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(starts_with("ds_nome_"), ~ ifelse(. == "0000", NA, .))) |> 
  vitallinkage::ajuste_res() |> 
  vitallinkage::soundex_linkage("ds_bairro_res") |>
  vitallinkage::soundex_linkage("ds_rua_res") |>
  vitallinkage::soundex_linkage("ds_comple_res") |>
  vitallinkage::soundex_linkage("ds_ref_res") |> 
  mutate(across(ends_with("_res2"), ~ ifelse(. == "", NA, .))) |> 
  mutate(across(ends_with("_res2_sound"), ~ ifelse(. == "0000", NA, .)))



# Função para decodificar a raça/cor
ds_raca <- function(df){
  df <- df |> mutate(ds_raca=
                       case_when(
                         cd_raca == 1 ~ "Branca", 
                         cd_raca == 2 ~ "Preta",
                         cd_raca == 3 ~ "Amarela",
                         cd_raca == 4 ~ "Parda",
                         cd_raca == 5 ~ "Indígena",
                         cd_raca == 9 ~ "Ignorada",
                         TRUE ~ "Ignorada"
                       )
  )
}
# Função para decodificar a descrição do grupo do agente tóxico
ds_agente_tox <- function(df){
  df <- df |> mutate(ds_agente_tox =
                       case_when(  
                         cd_agente_tox == "01" ~ "Medicamento",
                         cd_agente_tox == "02" ~ "Agrotóxico/uso agrícola", 
                         cd_agente_tox == "03" ~ "Agrotóxico/uso doméstico",
                         cd_agente_tox == "04" ~ "Agrotóxico/uso saúde pública", 
                         cd_agente_tox == "05" ~ "Raticida", 
                         cd_agente_tox == "06" ~ "Produto Veterinário",
                         cd_agente_tox == "07" ~ "Produto de uso domiciliar", 
                         cd_agente_tox == "08" ~ "Cosmético / higiene pessoal",
                         cd_agente_tox == "09" ~ "Produto químico de uso industrial",
                         cd_agente_tox == "10" ~ "Metal",
                         cd_agente_tox == "11" ~ "Drogas de abuso", 
                         cd_agente_tox == "12" ~ "Planta tóxica",
                         cd_agente_tox == "13" ~ "Alimento e bebida", 
                         cd_agente_tox == "14" ~ "Outro", 
                         cd_agente_tox == "99" ~ "Ignorado",
                         TRUE ~"Ignorado"))
}

# Função para decodificar o local de ocorrência da exposição
ds_loc_expo <- function(df){
  df <- df |> 
    mutate(
      ds_loc_expo =
        case_when(
          cd_loc_expo == "1" ~ "Residência", 
          cd_loc_expo == "2" ~ "Ambiente de Trabalho", 
          cd_loc_expo == "3" ~ "Trajeto do trabalho", 
          cd_loc_expo == "4" ~ "Serviços de saúde", 
          cd_loc_expo == "5" ~ "Escola/creche",
          cd_loc_expo == "6" ~ "Ambiente Externo",
          cd_loc_expo == "7" ~ "Outro",
          cd_loc_expo == "9" ~ "Ignorado",
          TRUE ~ "Ignorado"
        )
    )
  
}

# Função para decodificar a via de exposição primeira opção
ds_via_1 <- function(df){
  df <- df |> 
    mutate(
      ds_cd_via1 =
        case_when(
          cd_via1 == "1" ~ "Digestiva", 
          cd_via1 == "2" ~ "Cutânea", 
          cd_via1 == "3" ~ "Respiratória", 
          cd_via1 == "4" ~ "Ocular", 
          cd_via1 == "5" ~ "Parenteral", 
          cd_via1 == "6" ~ "Vaginal", 
          cd_via1 == "7" ~ "Transplacentária", 
          cd_via1 == "8" ~ "Outra", 
          cd_via1 == "9" ~ "Ignorado",
          TRUE ~ "Ignorado"
        ))
}

# Função para decodificar a via de exposição segunda opção
ds_via_2 <- function(df){
  df <- df |> 
    mutate(
      ds_via_2 =
        case_when(
          cd_via2 == "1" ~ "Digestiva", 
          cd_via2 == "2" ~ "Cutânea", 
          cd_via2 == "3" ~ "Respiratória", 
          cd_via2 == "4" ~ "Ocular", 
          cd_via2 == "5" ~ "Parenteral", 
          cd_via2 == "6" ~ "Vaginal", 
          cd_via2 == "7" ~ "Transplacentária", 
          cd_via2 == "8" ~ "Outra", 
          cd_via2 == "9" ~ "Ignorado",
          TRUE ~ "Ignorado"
        ))
}

# Função para decodificar a Circunstância da exposição
ds_circunstan <- function(df){
  df <- df |> 
    mutate(ds_circunstan =
             case_when(
               cd_circunstan == "01" ~ "Uso habitual", 
               cd_circunstan == "02" ~ "Acidental", 
               cd_circunstan == "03" ~ "Ambiental", 
               cd_circunstan == "04" ~ "Uso terapêutico", 
               cd_circunstan == "05" ~ "Prescrição médica inadequada", 
               cd_circunstan == "06" ~ "Erro de administração", 
               cd_circunstan == "07" ~ "Automedicação",
               cd_circunstan == "08" ~ "Abuso", 
               cd_circunstan == "09" ~ "Ingestão de alimento ou bebida", 
               cd_circunstan == "10" ~ "Tentativa de suicídio",
               cd_circunstan == "11" ~ "Tentativa de aborto", 
               cd_circunstan == "12" ~ "Violência / homicídio", 
               cd_circunstan == "13" ~ "Outra", 
               cd_circunstan == "99" ~ "Ignorado",
               TRUE ~ "Ignorado"
             )
    )
}

# Função para decodificar o Tipo de exposição
ds_tpexp <- function(df){
  df <- df |> 
    mutate(ds_tpexp =
             case_when(
               cd_tpexpo == "1" ~ "Aguda – única", 
               cd_tpexpo == "2" ~ "Aguda – repetida", 
               cd_tpexpo == "3" ~ "Crônica", 
               cd_tpexpo == "4" ~ "Aguda sobre crônica",
               cd_tpexpo == "9" ~ "Ignorado",
               TRUE ~ "Ignorado"
             )
    )
  
}


# Função para decodificar o tipo de atendimento
ds_tpatend <- function(df){
  df <- df |> 
    mutate(ds_tpatend =
             case_when(
               cd_tpatende == "1" ~ "Hospitalar", 
               cd_tpatende == "2" ~ "Ambulatorial", 
               cd_tpatende == "3" ~ "Domiciliar", 
               cd_tpatende == "4" ~ "Nenhum",
               cd_tpatende == "9" ~ "Ignorado",
               TRUE ~ "Ignorado"
             )
    )
  
}

# Função para decodificar se houve hospitalização
ds_hospital <- function(df){
  df <- df |> 
    mutate(ds_hospital =
             case_when(
               cd_hospitalizacao == "1" ~ "Sim", 
               cd_hospitalizacao == "2" ~ "Não", 
               cd_hospitalizacao == "9" ~ "Ignorado",
               TRUE ~ "Ignorado"
             )
    )
  
}

# Função para decodificar a classificação final
## DESCOBRIR O QUE É O 8
ds_classi_fin <- function(df){
  df <- df |> 
    mutate(
      ds_classi_fin =
        case_when(
          cd_classi_fin == "1" ~ "Intoxicação Confirmada", 
          cd_classi_fin == "2" ~ "Exposição", 
          cd_classi_fin == "3" ~ "Reação adversa", 
          cd_classi_fin == "4" ~ "Diagnóstico diferencial", 
          cd_classi_fin == "5" ~ "Síndrome de abstinência", 
          cd_classi_fin == "9" ~ "Ignorado",
          TRUE ~ cd_classi_fin
        ))
}


# Decodificando a variável de Evolução do caso
ds_evolucao <- function(df){
  df <- df |> 
    mutate(
      ds_evolucao =
        case_when(
          cd_evolucao == "1" ~ "Cura sem sequela", 
          cd_evolucao == "2" ~ "Cura com sequela", 
          cd_evolucao == "3" ~ "Óbito por intoxicação exógena", 
          cd_evolucao == "4" ~ "Óbito por outra causa", 
          cd_evolucao == "5" ~ "Perda do seguimento", 
          cd_evolucao == "9" ~ "Ignorado",
          TRUE ~ "Ignorado"
        ))
}



# Decodificando a variável de Evolução do caso
df_clean <- df_clean |> 
  ds_raca() |> 
  ds_agente_tox() |> 
  ds_loc_expo() |> 
  ds_via_1() |> 
  ds_via_2() |> 
  ds_circunstan() |> 
  ds_tpexp() |> 
  ds_tpatend() |> 
  ds_hospital() |> 
  ds_classi_fin() |> 
  ds_evolucao() |> 
  vitallinkage::nu_idade_anos_sinan()


sinan_iexo <- df_clean
rm(df_clean, names_sinan_iexo)

#-----------------------------------
# Adicionar ao banco de dados
#-----------------------------------

# 1. ‑‑ Cria somente a estrutura (0 linhas) -----------------------------
dbWriteTable(
  conn      = con,
  name      = SQL("tratado_sinan_iexo"),   # use SQL() para preservar maiúsculas/minúsculas
  value     = sinan_iexo[0, ],              # dataframe zerado, mantém tipos
  overwrite = TRUE,                         # recria se já existir
  row.names = FALSE,
  field.types = c(id_sinan_iexo = "BIGINT",
                  id_registro_linkage = "BIGINT")
)

# 2. ‑‑ Ajusta tipos/constraints depois que a tabela existe -------------
dbExecute(con, "
  ALTER TABLE tratado_sinan_iexo 
    ADD PRIMARY KEY (id_sinan_iexo)
")


# 3. ‑‑ Insere o conteúdo real (mantém o esquema) -----------------------
#tictoc::tic()
dbAppendTable(con, "tratado_sinan_iexo", sinan_iexo)
tictoc::toc()

