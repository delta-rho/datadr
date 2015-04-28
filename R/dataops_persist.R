#' Persist a Transformed 'ddo' or 'ddf' Object
#'
#' Persist a transformed 'ddo' or 'ddf' object
#'
#' @param x an object of class 'ddo' or 'ddf'
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#'
#' @return a 'ddo' or 'ddf' object
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{addTransform}}
#'
#' @examples
#' bySpecies <- divide(iris, by = "Species")
#' bySpeciesSepal <- bySpecies %>%
#'  addTransform(function(x) x[,c("Sepal.Length", "Sepal.Width")]) %>%
#'  drPersist()
#' @export
drPersist <- function(x, output = NULL, overwrite = FALSE, control = NULL) {
  if(!inherits(x, "transformed")) {
    message("This object has already been persisted")
    return(x)
  } else {
    res <- recombine(x, combine = combDdo(), output = output, overwrite = overwrite, control = control)
    if(inherits(x, "ddf")) {
      return(ddf(res))
    }
    res
  }
}

