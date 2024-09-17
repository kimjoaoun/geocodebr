#' Detect year from a single file path
#'
#' @param filename A local file path
#' @return A vector.
#'
#' @keywords internal
detect_year_from_string <- function(string){
  yyyy <- regmatches(string, gregexpr("\\d{4}", string))[[1]]
  yyyy <- unique(yyyy)
  yyyy <- yyyy[ yyyy != '2500' ]
  return(yyyy)
}

extract_url <- function(x){
  links <- RCurl::getURL(x) |>
    rvest::read_html() |>
    rvest::html_nodes(xpath = '//td/a[@href]') |>
    rvest::html_attr('href')
  return(links)
}



# function do detect the string of zipped files in url

list_zipfiles_in_url <- function(year) {

  if (year == 2022) {
    ftp <- c("https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/CSV/UF/")

    # harvest links
    links <- lapply(ftp,extract_url)
    links <- unlist(links)
    links <- unique(links)
    links <- links[links %like% 'zip']

    links <- c(paste0(ftp[1],"", links),
               paste0(ftp[2],"", links))

  }

  if (year == 2010) {
    ftp <- "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2010/"

    # harvest links
    links <- extract_url(ftp)
    links <- links[-1]
    links <- links[!links %like% 'zip']
    ftp = paste0(ftp, links)

    links <- lapply(ftp,extract_url)
    links <- unlist(links)
    links <- links[links %like% 'zip']
    links <- links[nchar(links) == 6]
    links <- paste0(ftp,"", links)

  }

  return(links)
}
