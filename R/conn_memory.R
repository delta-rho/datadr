# need dummy load and save methods

#' @S3method loadAttrs nullConn
loadAttrs.nullConn <- function(obj, type="ddo") {
   NULL
}

#' @S3method saveAttrs nullConn
saveAttrs.nullConn <- function(obj, attrs, type="ddo") {
   NULL
}

#' @S3method print nullConn
print.nullConn <- function(x) {
   cat("In-memory data connection")
}
