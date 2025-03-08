trata_empates_geocode <- function(output_df = parent.frame()$output_df,
                                  resolver_empates = parent.frame()$resolver_empates,
                                  verboso = parent.frame()$verboso) {

  # encontra casos de empate
  output_df[, empate := ifelse(.N > 1, TRUE, FALSE), by = tempidgeocodebr]

  # calcula distancias entre casos empatados
  output_df[empate == TRUE,
            dist_geocodebr := dt_haversine(
              lat, lon,
              data.table::shift(lat), data.table::shift(lon)
            ),
            by = tempidgeocodebr
            ]

  # coloca distancia 0 p/ primeiro caso
  output_df[empate == TRUE,
            dist_geocodebr := ifelse(is.na(dist_geocodebr), 0, dist_geocodebr)
            ]

  # ignora casos com dist menor do q 300 metros
  output_df2 <- output_df[ empate==FALSE |
                           empate==TRUE & dist_geocodebr == 0 |
                           dist_geocodebr > 300
                           ]

  # update casos de empate
  output_df2[, empate := ifelse(.N > 1, TRUE, FALSE), by = tempidgeocodebr]

  # conta numero de casos empatados
  ids_empate <- output_df2[empate == TRUE, ]$tempidgeocodebr
  n_casos_empate <- unique(ids_empate) |> length()

  # drop geocodebr temp columns
  output_df2[, dist_geocodebr := NULL]

  # se nao for para resolver empates:
  # - retorna resultado assim mesmo
  # - gera warning
  if (n_casos_empate >= 1 & isFALSE(resolver_empates)) {

    cli::cli_warn(
      "Foram encontrados {n_casos_empate} casos de empate. Estes casos foram marcados com valor igual `TRUE` na coluna 'empate',
       e podem ser inspecionados na coluna 'endereco_encontrado'. Alternativamente, use `resolver_empates==TRUE` para que o pacote
       lide com os empates automaticamente."
    )
  }

  # se for para resolver empates
  # - pega primeiro ponto para casos sem salvaco
  # - agrega casos provaveis de serem na mesma rua
  # - gera warning

  if (n_casos_empate >= 1 & isTRUE(resolver_empates)) {

    # Keeping only unique rows based on all columns except 'score',
    # selecting the row with the max 'score'
    output_df2 <- output_df2[output_df2[, .I[contagem_cnefe == max(contagem_cnefe)], by = tempidgeocodebr]$V1]
    output_df2 <- output_df2[output_df2[, .I[1], by = tempidgeocodebr]$V1]
    output_df2[, 'contagem_cnefe' := NULL]

    if (verboso) {
      plural <- ifelse(n_casos_empate==1, 'caso', 'casos')
      message(glue::glue(
        "Foram encontrados e resolvidos {n_casos_empate} {plural} de empate."
      ))
    }
  }

  return(output_df2)
}

# # empates <- filter(output_df2, empate ==T)
# # table(empates$tipo_resultado)
#
# # identifica casos sem solucao (e.g. RUA A) ---------------------------
# # para esses casos, deixa como esta
# # para os demais, tira media das coords, ponderada pelo cnefe count
#
# num_ext <- c(
#   'UM',
#   'DOIS',
#   'TRES',
#   'QUATRO',
#   'CINCO',
#   'SEIS',
#   'SETE',
#   'OITO',
#   'NOVE',
#   'DEZ',
#   'ONZE',
#   'DOZE',
#   'TREZE',
#   'QUATORZE',
#   'QUINZE',
#   'DEZESSEIS',
#   'DEZESSETE',
#   'DEZOITO' ,
#   'DEZENOVE',
#   'VINTE',
#   'TRINTA'
# )
#
#
# ruas_letras <- paste(paste("RUA", LETTERS), collapse = " |")
# ruas_numerais <- paste(paste("RUA", 1:30), collapse = " |")
# ruas_num_ext <- paste(paste("RUA", num_ext), collapse = " |")
#
# ruas_letras <- paste0(ruas_letras, " ")
# ruas_numerais <- paste0(ruas_numerais, " ")
# ruas_num_ext <- paste0(ruas_num_ext, " ")
#
# # separa casos
# ids_sem_empate <- output_df2[empate == FALSE]$tempidgeocodebr
#
# empates_perdidos <- output_df2[
#   empate == TRUE & (
#     endereco_encontrado %like% ruas_letras |
#       endereco_encontrado %like% ruas_numerais |
#       endereco_encontrado %like% ruas_num_ext) ]
#
# # # desconsiderar enderecos q sao datas (e.g. 'RUA QUINZE DE NOVEMBRO')
# # meses_pattern <- "\\b\\d{1,2} DE (JANEIRO|FEVEREIRO|MARÇO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\\b"
# # empates_perdidos <- empates_perdidos[grepl(meses_pattern, logradouro_encontrado)]
#
# empates_salve <- output_df2[empate == TRUE &
#                               (! tempidgeocodebr %in% c(ids_sem_empate, empates_perdidos$tempidgeocodebr))
#                             ]
#
