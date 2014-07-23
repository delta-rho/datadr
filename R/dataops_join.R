#' Join Two Data Sources by Key
#' 
#' Join two data sources by key
#' 
#' @param \ldots named lists of input objects - assumed that all are of same type (all HDFS, all localDisk, all in-memory)
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param postTransFn an optional function to be applied to the each final key-value pair after joining
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of parameters external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' 
#' @return a 'ddo' object stored in the \code{output} connection, where the values are named lists with names according to the names given to the input data objects, and values are the corresponding data
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
#' drJoin(Sepal.Width=sw, Sepal.Length=sl, postTransFn = as.data.frame)
#' 
#' @export
drJoin <- function(..., output = NULL, overwrite = FALSE, postTransFn = NULL, params = NULL, control = NULL) {
   # bySpecies <- divide(iris, by = "Species")
   # sw <- lapply(bySpecies, function(x) x$Sepal.Width)
   # sl <- lapply(bySpecies, function(x) x$Sepal.Length)
   # inputs <- list(Sepal.Width = sw, Sepal.Length = sl)
   # output <- NULL
   # control <- NULL
   # postTransFn <- as.data.frame
   
   inputs <- list(...)
   
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
      attr(res, "split") <- attr(res[[1]], "split")
      if(!is.null(postTransFn)) {
         res <- postTransFn(res)
      }
      collect(reduce.key, res)
   })
   
   globalVarList <- drGetGlobals(postTransFn)
   parList <- list(postTransFn = postTransFn)
   
   mrExec(inputs,
      map = map,
      reduce = reduce,
      control = control,
      output = output,
      overwrite = overwrite,
      params = c(parList, globalVarList, params)
   )
}

