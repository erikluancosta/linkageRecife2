start_linkage_dt <- function(df, variables) {
  # Início da contagem do tempo
  tictoc::tic("Tempo de execução")
  
  # Adicionando uma coluna de índice para manter o rastreamento das linhas originais
  df <- dplyr::mutate(df, original_index = dplyr::row_number())
  
  # Filtrando para registros que não possuem valores NA nas variáveis especificadas
  df_filtered <- dplyr::filter(df,
                               stats::complete.cases(
                                 dplyr::select(df, dplyr::all_of(variables))
                               )
  )
  
  # Transformando em data.table para melhor performance
  df_filtered <- data.table::as.data.table(df_filtered)
  
  # Criando uma nova coluna 'N_par' onde é contado o número de registros iguais baseado nas variáveis
  df_filtered[, N_par := .N, by = variables]
  
  # Filtrando os grupos com mais de uma ocorrência
  df_filtered <- df_filtered[N_par > 1]
  
  # Calculando 'par_1' que é um ID do grupo único para as combinações de variáveis
  df_filtered[, par_1 := .GRP, by = variables]
  
  # Ajustando par_1 para começar de 1 em diante
  unique_groups <- unique(df_filtered$par_1)
  df_filtered[, par_1 := match(par_1, unique_groups)]
  
  # Criando 'par_c1' que é simplesmente uma cópia de 'par_1' para manter consistência com a função R
  df_filtered[, par_c1 := par_1]
  
  # Convertendo de volta para data.frame e removendo duplicatas
  df_filtered <- dplyr::distinct(as.data.frame(df_filtered))
  
  # Mantendo apenas as colunas necessárias para o join, incluindo o índice original
  df_filtered <- dplyr::select(df_filtered, original_index, par_1, par_c1)
  
  # Reintegrando os registros filtrados ao DataFrame original usando o índice
  df <- dplyr::left_join(df, df_filtered, by = "original_index")
  
  # Removendo a coluna de índice original
  df <- dplyr::select(df, -original_index, par_1, dplyr::everything())
  
  # Fim da contagem do tempo
  tictoc::toc()
  
  return(df)
}




# ----------------------------
# Regras do linkage
# ----------------------------

library(data.table)

meio_de_campo_dt <- function(dt, max_iter = 3L) {
  setDT(dt)
  
  for (k in seq_len(max_iter)) {
    
    snap <- dt$par_1                               # 1.  estado antes da volta
    
    ## A. par_2 ▸ maior par_1 já conhecido ------------------------------------
    dt[!is.na(par_1) & !is.na(par_2),
       par1_max := max(par_1),
       by = par_2]
    
    ## B. par_final = coalesce( par1_max , par_1 , par_2 ) --------------------
    dt[, par_final := fcoalesce(par1_max, par_1, par_2)]
    
    ## C. coloca par_final onde par_1 ainda é NA ------------------------------
    dt[is.na(par_1) & !is.na(par_final), par_1 := par_final]
    
    ## D. fecha transitividade (SEM perder códigos) --------------------------
    dt[!is.na(par_1),
       par_1 := {
         m <- max(par_final, na.rm = TRUE)
         if (is.infinite(m)) .BY$par_1 else m      # se só NA, mantém o antigo
       },
       by = .(par_1)]
    
    ## limpa temporários desta volta
    dt[, c("par1_max", "par_final") := NULL]
    
    ## convergiu?
    if (identical(snap, dt$par_1)) break
  }
  
  ## Safety-check final: se ainda restou NA em par_1, usa par_2 (se houver)
  dt[is.na(par_1) & !is.na(par_2), par_1 := par_2]
  
  invisible(dt)
}

regras_linkage_dt <- function(df, variables, num_regra) {
  tictoc::tic("Tempo de processamento")
  setDT(df)
  if (!".rowid" %in% names(df)) df[, .rowid := .I]   # preserva ordem
  vars <- variables
  
  ## 1. linhas COMPLETAS p/ a regra ------------------------------------------
  idx_complete <- df[, which(complete.cases(.SD)), .SDcols = vars]
  if (!length(idx_complete)) return(df[order(.rowid)])
  
  ## 2. grupos com ≥ 2 ocorrências -------------------------------------------
  grupos <- df[idx_complete, .N, by = vars][N > 1L]
  if (!nrow(grupos)) return(df[order(.rowid)])
  
  ## 3. marca participantes ---------------------------------------------------
  df[, flag := FALSE]
  df[grupos, on = vars, flag := TRUE]
  
  ## 4. calcula par_1_new -----------------------------------------------------
  max_code <- ifelse(any(!is.na(df$par_1)), max(df$par_1, na.rm = TRUE), 0L)
  
  df[flag == TRUE,
     par_1_new := {
       cur <- par_1[!is.na(par_1)]
       if (length(cur)) max(cur) else NA_integer_
     },
     by = vars]
  
  df[flag == TRUE & is.na(par_1_new),
     par_1_new := max_code + .GRP,
     by = vars]
  
  ## 5. escreve par_cX e par_2  (AJUSTE AQUI)
  par_c <- paste0("par_c", num_regra)
  if (!par_c %in% names(df)) df[, (par_c) := NA_integer_]
  
  cols_to_set <- c(par_c, "par_2")
  df[flag == TRUE, (cols_to_set) := .(par_1_new, par_1_new)]
  
  ## 6. fechamento transitivo completo ---------------------------------------
  meio_de_campo_dt(df, max_iter = 3L)
  
  ## 7. limpeza --------------------------------------------------------------
  df[, c("flag", "par_1_new") := NULL]
  setorder(df, .rowid)[, .rowid := NULL]
  
  ## 8. checagem final -------------------------------------------------------
  if (all(is.na(df[[par_c]]))) message("Nenhum registro foi pareado")
  
  tictoc::toc()
  return(df[])
}

