# need dummy load and save methods

#' @export
loadAttrs.nullConn <- function(obj, type = "ddo") {
  NULL
}

#' @export
saveAttrs.nullConn <- function(obj, attrs, type = "ddo") {
  NULL
}

#' @export
print.nullConn <- function(x, ...) {
  cat("In-memory data connection")
}

#' @export
mrCheckOutputLoc.nullConn <- function(x, overwrite = FALSE)
  NULL

#' @export
mrCheckOutputLoc.NULL <- function(x, overwrite = FALSE)
  NULL
