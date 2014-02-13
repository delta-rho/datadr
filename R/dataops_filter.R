#' Filter a 'ddo' or 'ddf' Object
#' 
#' Filter a 'ddo' or 'ddf' object
#' 
#' @param x an object of class 'ddo' or 'ddf'
#' @param filterFn function that takes the keys and/or values and returns either \code{TRUE} or \code{FALSE} - if \code{TRUE}, that key-value pair will be present in the result
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' 
#' @return a 'ddo' or 'ddf' object
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{join}}
#' 
#' @export
drFilter <- function(x, filterFn, output = NULL, control = NULL) {
   # TODO: warn if output storage is not commensurate with input?
   # which will most-often happen when "output" is forgotten
   map <- expression({
      for(i in seq_along(map.keys)) {
         if(kvApply(list(map.keys[[i]], map.values[[i]])))
            collect(map.keys[[i]], map.values[[i]])
      }
   })
   
   mrExec(inputs,
      map = map,
      control = control,
      output = output,
      params = list(filterFn = filterFn)
   )
}
