
#' @export
rhipeControl <- function(mapred=NULL, combiner=FALSE, setup=NULL) {
   res <- list(mapred=mapred, combiner=combiner, setup=setup)
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
