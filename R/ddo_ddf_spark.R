## Methods for object of class "kvSparkData" - key-value pairs as Spark RDDs

#' @export
ddoInit.sparkDataConn <- function(obj, ...) {
   structure(list(), class="kvSparkData")
}

#' @export
ddoInitConn.sparkDataConn <- function(obj, ...) {
   obj
}

#' @export
requiredObjAttrs.kvSparkData <- function(obj) {
   list(
      ddo = getDr("requiredDdoAttrs"),
      ddf = getDr("requiredDdfAttrs")
   )
}

#' @export
getBasicDdoAttrs.kvSparkData <- function(obj, conn) {
   list(
      conn = conn,
      extractableKV = FALSE, 
      totStorageSize = NA, # TODO...
      nDiv = NA, # TODO,
      example = conn$data[[1]]
   )
}

#' @export
getBasicDdfAttrs.kvSparkData <- function(obj) {
   list(vars = lapply(kvExample(obj)[[2]], class))
}

# kvSparkData is never extractable (yet...)
#' @export
hasExtractableKV.kvSparkData <- function(x) {
   FALSE
}

######################################################################
### extract methods
######################################################################

#' @export
datadr_extract.kvSparkData <- function(x, i, ...) {
   stop("can't extract spark data by key yet...")
}

######################################################################
### convert methods
######################################################################

#' @export
convertImplemented.kvSparkData <- function(obj) {
   c("sparkDataConn", "NULL")
}

#' @export
convert.kvSparkData <- function(from, to=NULL) {
   convertkvSparkData(to, from)
}

convertkvSparkData <- function(obj, ...)
   UseMethod("convertkvSparkData", obj)

# from sparkData to sparkData
#' @export
convertkvSparkData.sparkDataConn <- function(to, from, verbose=FALSE) {
   from
}

# from sparkData to memory
#' @export
convertkvSparkData.NULL <- function(to, from, verbose=FALSE) {
   res <- getAttribute(from, "conn")$data
   
   if(inherits(from, "ddf")) {
      res <- ddf(res, update=FALSE, verbose=verbose)
   } else {
      res <- ddo(res, update=FALSE, verbose=verbose)
   }
   
   addNeededAttrs(res, from)
}


# # from sparkData to local disk
# #' @export
# convertkvSparkData.sparkDataConn <- function(to, from, verbose=FALSE) {
#    from
# }
# 
# # from sparkData to HDFS
# #' @export
# convertkvSparkData.hdfsConn <- function(to, from, verbose=FALSE) {
# }
# 
