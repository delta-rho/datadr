### general map/reduce methods

# the fundamental way to deal with divided data objects is mapreduce
# we generalize that so that if there is a new backend for divided
# data objects, we can simply implement map, reduce, and exec methods
# and then all datadr computations should work on the new backend

# map takes the input data and an expression
# which expects to have "map.keys" and "map.values" defined
# it also has a "collect" function

# reduce takes input data and an expression of pre, reduce, post
# it expects to have a "collect" function

#' Execute a MapReduce Job
#'
#' Execute a MapReduce job
#'
#' @param data
#' @param setup an expression of R code (created using the R command \code{expression}) to be run before map and reduce
#' @param map an R expression that is evaluated during the map stage. For each task, this expression is executed multiple times (see details).
#' @param reduce a vector of R expressions with names pre, reduce, and post that is evaluated during the reduce stage. For example \code{reduce = expression(pre={...}, reduce={...}, post={...})}. reduce is optional, and if not specified the map output key-value pairs will be the result. If it is not specified, then a default identity reduce is performed. Setting it to 0 will skip the reduce altogether.
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{\link{rhwatch}} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param params a named list of parameters external to the input data that are needed in the map or reduce phases
#' @param verbose 
#' 
#' @return "ddo" object - to keep it simple.  It is up to the user to update or cast as "ddf" if that is the desired result.
#' 
#' @author Ryan Hafen
#' 
#' @export
mrExec <- function(data, setup=NULL, map=NULL, reduce=NULL, output=NULL, control=NULL, params=NULL, verbose=TRUE) {
   require(digest)
   
   mrCheckInOut(data, output)
   
   if(is.null(control)) {
      control <- defaultControl(data)
   }
   
   # if map is NULL, replace with identity
   if(is.null(map))
      map <- expression({
         for(i in seq_along(map.keys))
            collect(map.keys[[i]], map.values[[i]])
      })
   
   # if reduce is NULL, don't do reduce
   # but if it's a number, n, do an identity reduce with n reduce tasks
   if(is.numeric(reduce))
      reduce <- expression({
         reduce = {
            collect(reduce.key, reduce.values)
         }
      })
   
   setup <- appendExpression(control$setup, setup)
   setup <- nullAttributes(setup)
   map <- nullAttributes(map)
   reduce <- nullAttributes(reduce)
   
   res <- mrExecInternal(data, setup=setup, map=map, reduce=reduce, output=output, control=control, params=params)
   
   obj <- ddo(res$data, update=FALSE) # if TRUE, can get recursive
   
   # extractableKV can change after any mr job
   obj <- setAttributes(obj, list(extractableKV = hasExtractableKV(obj), counters = res$counters))
   
   convert(obj, output)
}

mrExecInternal <- function(data, ...) {
   UseMethod("mrExecInternal", data)
}

defaultControl <- function(x) {
   UseMethod("defaultControl", x)
}

# check input and output
mrCheckInOut <- function(input, output) {
   if(!class(output)[1] %in% convertImplemented(input))
      stop("Cannot convert to requested output type")
}
