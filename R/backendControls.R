
#' @export
rhipeControl <- function(mapred=NULL, combiner=FALSE) {
   res <- list(mapred=mapred, combiner=combiner)
   class(res) <- "rhipeControl"
   res
}

#' @export
defaultControl <- function(x) {
   UseMethod("defaultControl", x)
}

#' @export
defaultControl.rhData <- function(x) {
   res <- list(mapred=NULL, combiner=FALSE)
   class(res) <- "rhipeControl"
   res
}

#' @export
defaultControl.localDiv <- function(x) {
   NULL
}
