#' Take a Sample of Key-Value Pairs
#' Take a sample of key-value Pairs
#' @param x a 'ddo' or 'ddf' object
#' @param fraction fraction of key-value pairs to keep (between 0 and 1)
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @export
#' @examples
#' bySpecies <- divide(iris, by = "Species")
#' set.seed(234)
#' sampleRes <- drSample(bySpecies, fraction = 0.25)
drSample <- function(x, fraction, output = NULL, overwrite = FALSE, control = NULL) {
  # TODO: warn if output storage is not commensurate with input?
  map <- expression({
    for(i in seq_along(map.keys)) {
      if(runif(1) < fraction)
        collect(map.keys[[i]], map.values[[i]])
    }
  })

  parList <- list(fraction = fraction)

  if(! "package:datadr" %in% search()) {
    parList <- c(parList, list(
      applyTransform = applyTransform,
      setupTransformEnv = setupTransformEnv,
      kvApply = kvApply
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
    params = parList
  )
}
