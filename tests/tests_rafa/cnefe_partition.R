## test cnefe partition -------------------------------
# streemap servidor

devtools::load_all('.')

# open input data
input_df <- arrow::read_parquet(system.file("extdata/large_sample.parquet", package = "geocodebr"))

dir_par <- 'C:/Users/r1701707/AppData/Local/R/cache/R/geocodebr/teste_particao2'
dir_nor <- 'C:/Users/r1701707/AppData/Local/R/cache/R/geocodebr/data_release_v0.1.0'

geocodebr::get_cache_dir()
geocodebr::set_cache_dir(dir_nor)
geocodebr::download_cnefe('all')

ncores <- 20

rafa_normal <- function(){

  geocodebr::set_cache_dir( dir_nor)

  df_rafa_nor <- geocodebr:::geocode_rafa(
    input_table = input_df,
    logradouro = "logradouro",
    numero = "numero",
    cep = "cep",
    bairro = "bairro",
    municipio = "municipio",
    estado = "uf",
    output_simple = F,
    n_cores= ncores,
    progress = T
  )
}

rafa_partition <- function(){

  geocodebr::set_cache_dir(dir_par)


  df_rafa_par <- geocodebr:::geocode_rafa(
    input_table = input_df,
    logradouro = "logradouro",
    numero = "numero",
    cep = "cep",
    bairro = "bairro",
    municipio = "municipio",
    estado = "uf",
    output_simple = F,
    n_cores= ncores,
    progress = T
  )
}


dani_normal <- function(){

  geocodebr::set_cache_dir( dir_nor)

  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )


  df_dani_nor <- geocodebr:::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F
  )
}


dani_partition <- function(){

  geocodebr::set_cache_dir(dir_par)

  fields <- geocodebr::setup_address_fields(
    logradouro = 'logradouro',
    numero = 'numero',
    cep = 'cep',
    bairro = 'bairro',
    municipio = 'municipio',
    estado = 'uf'
  )


  df_dani_par <- geocodebr:::geocode(
    addresses_table = input_df,
    address_fields = fields,
    n_cores = ncores,
    progress = F
  )
}

microbenchmark::microbenchmark(dani_nor = dani_normal(),
                               dani_par = dani_partition(),
                               rafa_nor = rafa_normal(),
                               rafa_par = rafa_partition(),
                               times = 5
)

# 10
#     expr      min       lq     mean   median       uq      max neval
# dani_nor 11.35229 14.01072 16.25409 15.28635 18.65661 22.97529    10
# dani_par 63.31290 64.35249 66.71211 66.64054 69.88589 70.70695    10
# rafa_nor 10.92875 11.28395 15.23715 15.74764 18.45543 19.82443    10
# rafa_par 51.20937 53.90713 56.75658 57.32128 59.07589 60.52053    10

# 1
#     expr      min       lq     mean   median       uq      max neval
# dani_nor 30.21568 30.93840 31.76746 30.94988 32.00688 34.72646     5
# dani_par 72.33863 72.94179 73.67241 74.18073 74.25940 74.64150     5
# rafa_nor 27.63571 30.12863 31.42922 30.95021 31.42592 37.00564     5
# rafa_par 63.12963 64.28583 65.47106 64.92844 67.02854 67.98284     5
