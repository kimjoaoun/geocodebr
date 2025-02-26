#' Busca por CEP
#'
#' Busca endereços e coordendas geográficas a partir de um CEP. As coordenadas
#' de output utilizam o sistema de referência geodésico "SIRGAS2000", CRS(4674).
#'
#' @param cep Vetor. Um CEP ou um vetor de CEPs com 8 dígitos.
#' @param resultado_sf Lógico. Indica se o resultado deve ser um objeto espacial
#'    da classe `sf`. Por padrão, é `FALSE`, e o resultado é um `data.frame` com
#'    as colunas `lat` e `lon`.
#' @template verboso
#' @template cache
#' @template n_cores
#'
#' @return Retorna um `data.frame` com os CEPs de input e os endereços presentes
#'   naquele CEP com suas coordenadas geográficas de latitude (`lat`) e
#'   longitude (`lon`). Alternativamente, o resultado pode ser um objeto `sf`.
#'
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' library(geocodebr)
#'
#' # ler amostra de dados
#' ceps <- c("70355-030", "71665-015", "70070-120", "09868-100")
#'
#' df <- geocodebr::busca_por_cep(
#'   cep = ceps,
#'   verboso = FALSE
#'   )
#'
#' head(df)
#'
#' @export
busca_por_cep_arrw <- function(cep,
                               resultado_sf = FALSE,
                               verboso = TRUE,
                               cache = TRUE,
                               n_cores = 1){

  # check input
  checkmate::assert_vector(cep)
  checkmate::assert_logical(resultado_sf, any.missing = FALSE, len = 1)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)


  # normalize input data -------------------------------------------------------

  cep_padrao <- enderecobr::padronizar_ceps(cep)



  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_cep_localidade.parquet")
  cnefe <- arrow_open_dataset( path_to_parquet )

  # filtrar por uf

  # # creating a temporary db and register the input table data
  # con <- create_geocodebr_db(n_cores = n_cores)


  output_df <- cnefe |>
    dplyr::select(cep, estado, municipio, logradouro, localidade, lat, lon) |>          # Drop the n_casos column
    dplyr::filter(cep %in% cep_padrao) |>
    dplyr::collect()


  # add any missing cep
  missing_cep <- cep_padrao[!cep_padrao %in% output_df$cep]

  if (length(missing_cep) == length(cep_padrao)) {
    cli::cli_abort("Nenhum CEP foi encontrado.")
  }
  if (length(missing_cep)>0) {
    temp_dt <- data.table::data.table(cep= missing_cep)
    output_df <- data.table::rbindlist(list(output_df, temp_dt), fill = TRUE)
  }

  # convert df to simple feature
  if (isTRUE(resultado_sf)) {
    output_sf <- sfheaders::sf_point(
      obj = output_df,
      x = 'lon',
      y = 'lat',
      keep = TRUE
    )

    sf::st_crs(output_sf) <- 4674
    return(output_sf)
  }

  return(output_df)
}



busca_por_cep_duck <- function(cep,
                               resultado_sf = FALSE,
                               verboso = TRUE,
                               cache = TRUE,
                               n_cores = 1){

  # check input
  checkmate::assert_vector(cep)
  checkmate::assert_logical(resultado_sf, any.missing = FALSE, len = 1)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)


  # normalize input data -------------------------------------------------------

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (verboso) message_standardizing_addresses()

  cep_padrao <- enderecobr::padronizar_ceps(cep)



  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_cep_localidade.parquet")
  cnefe <- arrow_open_dataset( path_to_parquet )

  # filtrar por uf

  # # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)
  duckdb::duckdb_register_arrow(con, 'cnefe', cnefe)



  # filter ceps of interest
  output_df <- dplyr::tbl(con, "cnefe") |>
    dplyr::select(cep, estado, municipio, logradouro, localidade, lat, lon) |>
    dplyr::filter(cep %in% cep_padrao) |>
    dplyr::collect()

  duckdb::duckdb_unregister_arrow(con, "cnefe")

  # add any missing cep
  missing_cep <- cep_padrao[!cep_padrao %in% output_df$cep]

  if (length(missing_cep) == length(cep_padrao)) {
    cli::cli_abort("Nenhum CEP foi encontrado.")
  }

  if (length(missing_cep)>0) {
    temp_dt <- data.table::data.table(cep= missing_cep)
    output_df <- data.table::rbindlist(list(output_df, temp_dt), fill = TRUE)
  }

  # convert df to simple feature
  if (isTRUE(resultado_sf)) {
    output_sf <- sfheaders::sf_point(
      obj = output_df,
      x = 'lon',
      y = 'lat',
      keep = TRUE
    )

    sf::st_crs(output_sf) <- 4674
    return(output_sf)
  }

  return(output_df)
}



busca_por_cep_sql <- function(cep,
                              resultado_sf = FALSE,
                              verboso = TRUE,
                              cache = TRUE,
                              n_cores = 1){

  # check input
  checkmate::assert_vector(cep)
  checkmate::assert_logical(resultado_sf, any.missing = FALSE, len = 1)
  checkmate::assert_logical(verboso, any.missing = FALSE, len = 1)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1)
  checkmate::assert_number(n_cores, lower = 1, finite = TRUE)


  # normalize input data -------------------------------------------------------

  # standardizing the addresses table to increase the chances of finding a match
  # in the CNEFE data

  if (verboso) message_standardizing_addresses()

  cep_padrao <- enderecobr::padronizar_ceps(cep)



  # download cnefe  -------------------------------------------------------

  # downloading cnefe
  cnefe_dir <- download_cnefe(
    verboso = verboso,
    cache = cache
  )


  # Load CNEFE data and filter it to include only states
  # present in the input table, reducing the search scope
  # Narrow search global scope of cnefe to bounding box
  path_to_parquet <- paste0(listar_pasta_cache(), "/municipio_logradouro_cep_localidade.parquet")
  cnefe <- arrow_open_dataset( path_to_parquet )

  # filtrar por uf

  # # creating a temporary db and register the input table data
  con <- create_geocodebr_db(n_cores = n_cores)
  duckdb::duckdb_register_arrow(con, 'cnefe', cnefe)

  query_filter <- glue::glue_sql(
    .con = con,
    "SELECT
        cep,
        estado,
        municipio,
        logradouro,
        localidade,
        lat,
        lon
     FROM cnefe
      WHERE cep IN ({cep_padrao*});"
  )

  # Execute the query on the DuckDB connection
  output_df <- DBI::dbGetQuery(con, query_filter)

  duckdb::duckdb_unregister_arrow(con, "cnefe")

  # add any missing cep
  missing_cep <- cep_padrao[!cep_padrao %in% output_df$cep]

  if (length(missing_cep) == length(cep_padrao)) {
    cli::cli_abort("Nenhum CEP foi encontrado.")
  }

  if (length(missing_cep)>0) {
    temp_dt <- data.table::data.table(cep= missing_cep)
    output_df <- data.table::rbindlist(list(output_df, temp_dt), fill = TRUE)
  }

  # convert df to simple feature
  if (isTRUE(resultado_sf)) {
    output_sf <- sfheaders::sf_point(
      obj = output_df,
      x = 'lon',
      y = 'lat',
      keep = TRUE
    )

    sf::st_crs(output_sf) <- 4674
    return(output_sf)
  }

  return(output_df)
}


cep1 <- '71665-015'
cep_null <- '09868-100'
cepM <- c("08391-588", "08391-509", "08391-585", "08391-110", "01453-000", "01454-000",
          "04476-270", "05735-080", "04205-000", "03461-010", "03361-010", "03360-010",
          "05547-025", "04771-180", "03962-120", "08431-160", "05633-000", "05633-010",
          "08030-000", "08030-430", "02269-000", "02269-010", "04218-005", "05613-001",
          "05593-001", "05307-120", "05861-260", "02942-000", "01139-002", "05037-030",
          "05866-000", "05866-170", "05870-140", "02072-001", "02072-000", "02072-002",
          "02135-001", "02135-002", "03405-110", "03560-190", "03021-055", "03704-000",
          "03077-005", "03605-030", "03086-035", "03074-000", "08150-310", "02918-170",
          "03402-000", "03403-000", "03402-001", "03403-001", "03402-002", "03403-002",
          "03402-003", "02430-001", "02430-002", "04014-011", "04014-010", "04014-001",
          "04014-002", "05581-000", "05582-000", "05581-001", "05582-001", "05339-000",
          "05339-004", "05339-001", "05339-003", "05339-002", "05340-903", "05340-002",
          "08245-470", "08235-770", "04474-410", "04651-000", "04295-001", "04295-000",
          "02442-000", "04773-000", "05118-040", "02306-005", "02306-003", "02368-000",
          "09868-100")

cepM <- c(cepM,cepM,cepM)

bench::mark(
#  sql1 = busca_por_cep_sql( cep = cepM, n_cores = 1),
  sql4 = busca_por_cep_sql( cep = cepM, n_cores = 4),
#  arrw1 = busca_por_cep_arrw(cep = cepM, n_cores = 1),
  arrw4 = busca_por_cep_arrw(cep = cepM, n_cores = 4),
#  duck1 = busca_por_cep_duck(cep = cepM, n_cores = 1),
  duck4 = busca_por_cep_duck(cep = cepM, n_cores = 4),
  iterations = 10,
  check = F
)

# 1 cep
#    expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#   1 arrw          390ms    403ms      2.47     204KB    0.275     9     1      3.64s <NULL>
#   2 duck          449ms    461ms      2.16     665KB    0.925     7     3      3.24s <NULL>
#   3 sql           390ms    397ms      2.49     166KB    0.277     9     1      3.61s <NULL>


# 84 ceps
#     expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#   1 arrw          186ms    225ms      4.48    19.4MB    1.12      8     2      1.78s <NULL>
#   2 duck          638ms    679ms      1.46      11MB    0.365     8     2      5.49s <NULL>
#   3 sql           528ms    592ms      1.67   578.1KB    0.185     9     1      5.39s <NULL>

#   1 sql           560ms    629ms      1.60   13.17MB    0.401     8     2      4.99s <NULL>
#   2 arrw          217ms    227ms      4.37    8.49MB    2.91      6     4      1.37s <NULL>
#   3 duck          612ms    630ms      1.60    9.43MB    3.73      3     7      1.88s <NULL>


# 255 ceps
#     expression      min   median `itr/sec` mem_alloc `gc/sec` n_itr  n_gc total_time result
#   1 sql1          475ms    504ms      1.98    13.2MB    0.220     9     1      4.55s <NULL>
#   2 sql4          427ms    495ms      2.08   436.8KB    0.231     9     1      4.34s <NULL>
#   3 arrw1         187ms    206ms      4.89     8.4MB    0.544     9     1      1.84s <NULL>
#   4 arrw4         180ms    181ms      5.12   266.8KB    0.569     9     1      1.76s <NULL>
#   5 duck1         679ms    751ms      1.29     9.4MB    0.551     7     3      5.45s <NULL>
#   6 duck4         517ms    578ms      1.70   846.1KB    0.426     8     2      4.69s <NULL>

