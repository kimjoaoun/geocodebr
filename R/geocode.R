#' Download microdata of population records from Brazil's census
#'
#' @description
#' Download microdata of population records from Brazil's census. Data collected
#' in the sample component of the questionnaire.
#'
#' @template year
#' @template columns
#' @template add_labels
#' @template as_data_frame
#' @template showProgress
#' @template cache
#'
#' @return An arrow `Dataset` or a `"data.frame"` object.
#' @export
#' @family Microdata
#' @examplesIf identical(tolower(Sys.getenv("NOT_CRAN")), "true")
#' # return data as arrow Dataset
#' df <- geocode(year = 2010,
#'                       showProgress = FALSE)
#'
#'
geocode <- function(input_table,
                    logradouro = NULL,
                    numero = NULL,
                    complemento = NULL,
                    cep = NULL,
                    bairro = NULL,
                    municipio = NULL,
                    estado = NULL,
                    showProgress = TRUE,
                    cache = TRUE){

  # check input
  checkmate::assert_data_frame(input_table)

  # prepare user input
  input_table <- data.frame(
    logradouro = "r ns sra da piedade",
    nroLogradouro = 20,
    complemento = "qd 20",
    cep = 25220020,
    bairro = "jd botanico",
    codmun_dom = 3304557,
    uf_dom = "rj"
  )

  campos <- enderecopadrao::correspondencia_campos(
    logradouro = "logradouro",
    numero = "nroLogradouro",
    complemento = "complemento",
    cep = "cep",
    bairro = "bairro",
    municipio = "codmun_dom",
    estado = "uf_dom"
  )

  input_padrao <- enderecopadrao::padronizar_enderecos(
    enderecos = input_table,
    campos_do_endereco = campos
    )



  # # states present in the input
  # input_states <- unique(input_padrao$estado)
  # # input_states <- c("AC", "AL", "RJ")
  #
  # ### 1 download cnefe no cache
  # download_cnefe(input_states)
  #
  # ### 2 padroniza input do usuario
  #   funcao dedicada
  #
  # ### 3 geocode deterministico
  # ## bloco 1 todas vars
  #   funcao dedicada
  #
  # ## bloco 2 todas vars - 1
  #   funcao dedicada
  #
  # ## bloco 3 todas vars - 2
  #   funcao dedicada
  #
  #   # futuro
  #   - join probabilistico
  #   - interpolar numeros na mesma rua





}

