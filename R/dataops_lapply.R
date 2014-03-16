# lapply-like operations can be completely accomplished with recombine
# but it's still nice to have an lapply method for when that is the intent

#' Apply a function to all key/value pairs of a ddo/ddf object
#' 
#' Apply a function to all key/value pairs of a ddo/ddf object and get a new ddo object back, unless a different \code{combine} strategy is specified.
#' 
#' @param data an object of class "ddo" of "ddf"
#' @param apply a function to be applied to each subset
#' @param combine optional method to combine the results
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of parameters external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param verbose logical - print messages about what is being done
#' @return depends on \code{combine}
#' @author Ryan Hafen
#' @seealso \code{\link{recombine}}, \code{\link{drFilter}}, \code{\link{drJoin}}, \code{\link{combDdo}}, \code{\link{combRbind}}
#' @export
#' @examples
#' bySpecies <- divide(iris, by="Species")
#' drLapply(bySpecies, function(x) x$Sepal.Width)
drLapply <- function(data, apply = NULL, combine = combDdo(), output = NULL, overwrite = FALSE, params = NULL, control = NULL, verbose = TRUE) {
   recombine(data = data, apply = apply, combine = combine, output = output, overwrite = overwrite, params = params, control = control, verbose = verbose)
}

