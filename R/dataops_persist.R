#' Persist a Transformed 'ddo' or 'ddf' Object
#'
#' Persist a transformed 'ddo' or 'ddf' object by making a deferred transformation permanent
#'
#' @param x an object of class 'ddo' or 'ddf'
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#'
#' @return a 'ddo' or 'ddf' object with the transformation evaluated on the data
#'
#' @details When a transformation is added to a ddf/ddo via \code{\link{addTransform}}, the transformation is deferred until
#' the some action is taken with the data (e.g. a call to \code{\link{recombine}}).  See the documentation of
#' \code{\link{addTransform}} for more information about the nature of transformations.
#'
#' Calling \code{drPersis()} on the ddo/ddf makes the transformation permanent (persisted).  In the case of a local disk
#' connection (via \code{\link{localDiscConn}}) or HDFS connection (via \code{\link{hdfsConn}}), the transformed data
#' are written to disc.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{addTransform}}
#'
#' @examples
#' bySpecies <- divide(iris, by = "Species")
#'
#' # Create the transformation and add it to bySpecies 
#' bySpeciesSepal <- addTransform(bySpecies, function(x) x[,c("Sepal.Length", "Sepal.Width")])
#'
#' # Note the transformation is 'pending' a data action 
#' bySpeciesSepal
#'
#' # Make the tranformation permanent (persistent)
#' bySpeciesSepalPersisted <- drPersist(bySpeciesSepal)
#'
#' # The transformation no longer pending--but a permanent part of the new ddo
#' bySpeciesSepalPersisted
#' bySpeciesSepalPersisted[[1]]
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

