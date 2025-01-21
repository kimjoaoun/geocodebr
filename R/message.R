geocodebr_message <- function(message, .envir = parent.frame()) {
  message_call <- sys.call(-1)
  message_function <- as.name(message_call[[1]])

  message_classes <- c(
    paste0("geocodebr_message_", sub("^message_", "", message_function)),
    "geocodebr_message"
  )

  cli::cli_inform(message, class = message_classes, .envir = .envir)
}

message_standardizing_addresses <- function() {
  geocodebr_message(c("i" = "Padronizando endereços de entrada"))
}

message_looking_for_matches <- function() {
  geocodebr_message(c("i" = "Geolocalizando endereços"))
}
