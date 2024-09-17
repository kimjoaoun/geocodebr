library(targets)
library(rvest)
library(arrow)
library(data.table)
library(RCurl)
library(dplyr)
library(curl)
library(fs)
library(enderecopadrao)
library(archive)

data.table::setDTthreads(percent = 100)
options(scipen = 999)




#### 1) download raw data from IBGE ftp -------------------------------------------

source('./R/utils.R')


# get url of all uf fils
cnefe_2022_urls <- list_zipfiles_in_url(year = 2022)
cnefe_2022_urls <- cnefe_2022_urls[cnefe_2022_urls %like% 'https://ftp.ibge']

# dest dir and files
dest_raw <- '../../geocodebr_data_prep/2022/raw/'
dir.create(dest_raw, recursive = TRUE)
dest_files <-  fs::path(dest_raw, basename(cnefe_2022_urls))


# download files
curl::multi_download(
  urls = cnefe_2022_urls,
  destfiles = dest_files,
  resume = TRUE,
  progress = TRUE
  )



#### 2) standardize cenfe ans save to parquet hive -------------------------------------------

# For each file, unzip to temp, standardize addresses, save as parquet
prep_cnefe <- function(zip_file){

  # zip_file = dest_files[1]

  # get file name

  # unzip
  temp_dir <- fs::path_temp()
  archive::archive_extract(archive = zip_file,
                           dir = temp_dir
  )

  # get file name
  fname <- fs::path_ext_remove(basename(zip_file))
  fname <- paste0(fname, '.csv')
  fname <- fs::path(temp_dir, fname)

  # read
  temp_df <- data.table::fread(fname)

  data.table::setnames(temp_df,
                       old = names(temp_df),
                       new = tolower(names(temp_df)))

  ### standardize addresses

  # add logradouro
  temp_df[, logradouro := paste0(nom_tipo_seglogr, nom_titulo_seglogr, nom_seglogr)]

  # add abbrev_state
  temp_df[, abbrev_state := fcase(
    cod_uf  == 11, "RO",
    cod_uf  == 12, "AC",
    cod_uf  == 13, "AM",
    cod_uf  == 14, "RR",
    cod_uf  == 15, "PA",
    cod_uf  == 16, "AP",
    cod_uf  == 17, "TO",
    cod_uf  == 21, "MA",
    cod_uf  == 22, "PI",
    cod_uf  == 23, "CE",
    cod_uf  == 24, "RN",
    cod_uf  == 25, "PB",
    cod_uf  == 26, "PE",
    cod_uf  == 27, "AL",
    cod_uf  == 28, "SE",
    cod_uf  == 29, "BA",
    cod_uf  == 31, "MG",
    cod_uf  == 32, "ES",
    cod_uf  == 33, "RJ",
    cod_uf  == 35, "SP",
    cod_uf  == 41, "PR",
    cod_uf  == 42, "SC",
    cod_uf  == 43, "RS",
    cod_uf  == 50, "MS",
    cod_uf  == 51, "MT",
    cod_uf  == 52, "GO",
    cod_uf  == 53, "DF",
    default = NA)]

  # set correspondent fields
  campos <- correspondencia_campos(
    logradouro = "logradouro",
    numero = "num_endereco",
    complemento = "nom_comp_elem1",
    cep = "cep",
    bairro = "dsc_localidade", # ????????????
    municipio = "cod_municipio",
    estado = "abbrev_state"
  )

  # standardize cnefe
  cnefe_std <- padronizar_enderecos(enderecos = temp_df,
                                    campos_do_endereco = campos)

  # recover variables
  cnefe_std[, abbrev_state := temp_df$abbrev_state]
  cnefe_std[, lon := temp_df$longitude]
  cnefe_std[, lat := temp_df$latitude]
  cnefe_std[, lat := temp_df$cod_setor]
  cnefe_std[, lat := temp_df$num_quadra]
  cnefe_std[, lat := temp_df$num_face]
  cnefe_std[, lat := temp_df$nv_geo_coord]
  cnefe_std[, lat := temp_df$cod_especie]

  # other variables we might use ?????

  # save parquet
  uf <- cnefe_std$abbrev_state[1L]
  dest_dir_parquet <- '../../geocodebr_data_prep/2022/parquet/'
  dir.create(dest_dir_parquet, recursive = TRUE)

  cnefe_std |>
    group_by(abbrev_state) |>
    arrow::write_dataset(path = dest_dir_parquet,
                         basename_template = sprintf("part-%s-{i}.parquet", uf),
                         hive_style = TRUE,
                         format = 'parquet')

}

# generate gecodebr version of cnefe
pbapply::pblapply(X=dest_files,
                  FUN = prep_cnefe)


### 3) zip parquet hive structure  ---------------------------------------------

dest_dir_parquet <- '../../geocodebr_data_prep/2022/parquet/'
all_parquet_dirs <- list.dirs(path = dest_dir_parquet)
all_parquet_dirs <- all_parquet_dirs[-1]

zip_hive <- function(directory_to_zip){

  # directory_to_zip <- all_parquet_dirs[1]

  # create hive dir zip
  zip_file_name <- basename(directory_to_zip)
  zip_file_name <- paste0(zip_file_name, '.zip')
  zip_file_name <- fs::path(fs::path_dir(directory_to_zip), zip_file_name)
  zip_file_name <- gsub('=','_', zip_file_name)

  # Construct the command
  zip::zip(zipfile = zip_file_name,
           files = list.files(directory_to_zip,full.names = T))

        #
        #   archive::archive_write_dir(
        #     archive = zip_path,
        #     dir = dir,
        #     recursive = T,
        #     full.names = FALSE)

          #   # add file
        #   archive_write(archive = zip_path,
        #                 file = list.files(dir, full.names = TRUE))
        #
        #   archive(zip_path)
}

pbapply::pblapply(X=all_parquet_dirs, FUN =zip_hive)




### 4) upload zips to github -----------------------------
zips_to_upoad <- list.files('../../geocodebr_data_prep/2022/parquet/',
                            pattern = '.zip',
                            full.names = TRUE)

zips_to_upoad <- zips_to_upoad[-3]

piggyback::pb_upload(file = zips_to_upoad,
                     repo = 'ipeaGIT/geocodebr',
                     tag = 'data_v0.0.1')


