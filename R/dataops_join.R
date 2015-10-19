#' Join Data Sources by Key
#'
#' Outer join of two or more distributed data object (DDO) sources by key
#'
#' @param \ldots Input data sources: two or more named DDO objects that will be joined, separated by commas (see Examples for syntax).
#' Specifically, each input object should inherit from the 'ddo' class.
#' It is assumed that all input sources are of same type (all HDFS, all localDisk, all in-memory). 
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param postTransFn an optional function to be applied to the each final key-value pair after joining
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#'
#' @return a 'ddo' object stored in the \code{output} connection, where the values are named lists with names according to the names given to the input data objects, and values are the corresponding data.
#' The 'ddo' object contains the union of all the keys contained in the input 'ddo' objects specified in \code{\ldots}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{drFilter}}, \code{\link{drLapply}}
#'
#' @examples
#' bySpecies <- divide(iris, by = "Species")
#' # get independent lists of just SW and SL
#' sw <- drLapply(bySpecies, function(x) x$Sepal.Width)
#' sl <- drLapply(bySpecies, function(x) x$Sepal.Length)
#' drJoin(Sepal.Width = sw, Sepal.Length = sl, postTransFn = as.data.frame)
#'
#' @export
drJoin <- function(..., output = NULL, overwrite = FALSE, postTransFn = NULL, params = NULL, packages = NULL, control = NULL) {

  inputs <- list(...)

  # Check the structure of 'inputs'.  At this point, it should be a list of objects that inherit
  # from ddo
  if(!all(sapply(inputs, inherits, what = "ddo"))) {
    stop("'...' must be one or more objects that inherit from the 'ddo' class")
  }
  
  map <- expression({
    for(i in seq_along(map.keys)) {
      v <- list(map.values[[i]])
      names(v) <- .dataSourceName
      collect(map.keys[[i]], v)
    }
  })

  reduce <- expression(pre = {
    res <- list()
  }, reduce = {
    res[length(res) + seq_along(reduce.values)] <- reduce.values
  }, post = {
    res <- unlist(res, recursive = FALSE)
    # all should have same split attribute
    # set it globally and get rid of it individually
    attr(res, "split") <- attr(res[[1]], "split")
    for(i in seq_along(res)) {
      attr(res[[i]], "split") <- NULL
    }
    if(!is.null(postTransFn)) {
      res <- postTransFn(res)
    }
    collect(reduce.key, res)
  })

  globalVarList <- drGetGlobals(postTransFn)
  parList <- list(postTransFn = postTransFn)

  # if the user supplies output as an unevaluated connection
  # the verbosity can be misleading
  suppressMessages(output <- output)

  mrExec(inputs,
    map = map,
    reduce = reduce,
    control = control,
    output = output,
    overwrite = overwrite,
    params = c(parList, globalVarList$vars, params),
    packages = c(globalVarList$packages, packages)
  )
}

