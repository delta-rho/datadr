#' Filter a 'ddo' or 'ddf' Object
#'
#' Filter a 'ddo' or 'ddf' object
#'
#' @param x an object of class 'ddo' or 'ddf'
#' @param filterFn function that takes the keys and/or values and returns either \code{TRUE} or \code{FALSE} - if \code{TRUE}, that key-value pair will be present in the result
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of parameters external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#'
#' @return a 'ddo' or 'ddf' object
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{drJoin}}, \code{\link{drLapply}}
#'
#' @examples
#' bySpecies <- divide(iris, by = "Species")
#' drFilter(bySpecies, function(v) mean(v$Sepal.Width) < 3)
#' @export
drFilter <- function(x, filterFn, output = NULL, overwrite = FALSE, params = NULL, packages = NULL, control = NULL) {
  # TODO: warn if output storage is not commensurate with input?
  # which will most-often happen when "output" is forgotten
  # TODO: check filterFn on a subset and make sure it returns a logical
  map <- expression({
    for(i in seq_along(map.keys)) {
      if(kvApply(filterFn, list(map.keys[[i]], map.values[[i]])))
        collect(map.keys[[i]], map.values[[i]])
    }
  })

  globalVarList <- drGetGlobals(filterFn)
  parList <- list(filterFn = filterFn, kvApply = kvApply)

  if(! "package:datadr" %in% search()) {
    parList <- c(parList, list(
      applyTransform = applyTransform,
      setupTransformEnv = setupTransformEnv
    ))
  }

  # if the user supplies output as an unevaluated connection
  # the verbosity can be misleading
  suppressMessages(output <- output)

  mrExec(x,
    map = map,
    control = control,
    output = output,
    overwrite = overwrite,
    params = c(parList, globalVarList$vars, params),
    packages = c(globalVarList$packages, packages)
  )
}

