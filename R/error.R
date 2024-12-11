geocodebr_error <- function(message, call, .envir = parent.frame()) {
  error_call <- sys.call(-1)
  error_function <- as.name(error_call[[1]])

  error_classes <- c(
    paste0("geocodebr_error_", sub("^error_", "", error_function)),
    "geocodebr_error"
  )

  cli::cli_abort(message, class = error_classes, call = call, .envir = .envir)
}
