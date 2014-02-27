## Methods for object of class "kvSparkData" - key-value pairs as Spark RDDs

#' @S3method ddoInit sparkDataConn
ddoInit.sparkDataConn <- function(obj, ...) {
   structure(list(), class="kvSparkData")
}

#' @S3method ddoInitConn sparkDataConn
ddoInitConn.sparkDataConn <- function(obj, ...) {
   obj
}

#' @S3method requiredObjAttrs kvSparkData
requiredObjAttrs.kvSparkData <- function(obj) {
   list(
      ddo = getDr("requiredDdoAttrs"),
      ddf = getDr("requiredDdfAttrs")
   )
}

#' @S3method getBasicDdoAttrs kvSparkData
getBasicDdoAttrs.kvSparkData <- function(obj, conn) {
   list(
      conn = conn,
      extractableKV = FALSE, 
      totSize = NA, # TODO...
      nDiv = NA, # TODO,
      example = conn$data[[1]]
   )
}

#' @S3method getBasicDdfAttrs kvSparkData
getBasicDdfAttrs.kvSparkData <- function(obj, transFn) {
   list(
      vars = lapply(kvExample(obj)[[2]], class),
      transFn = transFn
   )
}

# kvSparkData is never extractable (yet...)
#' @S3method hasExtractableKV kvSparkData
hasExtractableKV.kvSparkData <- function(x) {
   FALSE
}

######################################################################
### extract methods
######################################################################

#' @S3method [ kvSparkData
`[.kvSparkData` <- function(x, i, ...) {
   stop("can't extract spark data by key yet...")
}

#' @S3method [[ kvSparkData
`[[.kvSparkData` <- function(x, i, ...) {
   stop("can't extract spark data by key yet...")
}


######################################################################
### convert methods
######################################################################

#' @S3method convertImplemented kvSparkData
convertImplemented.kvSparkData <- function(obj) {
   c("sparkDataConn", "NULL")
}

#' @S3method convert kvSparkData
convert.kvSparkData <- function(from, to=NULL) {
   convertkvSparkData(to, from)
}

convertkvSparkData <- function(obj, ...)
   UseMethod("convertkvSparkData", obj)

# from sparkData to sparkData
#' @S3method convertkvSparkData sparkDataConn
convertkvSparkData.sparkDataConn <- function(to, from, verbose=FALSE) {
   from
}

# from sparkData to memory
#' @S3method convertkvSparkData NULL
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
# #' @S3method convertkvSparkData sparkDataConn
# convertkvSparkData.sparkDataConn <- function(to, from, verbose=FALSE) {
#    from
# }
# 
# # from sparkData to HDFS
# #' @S3method convertkvSparkData hdfsConn
# convertkvSparkData.hdfsConn <- function(to, from, verbose=FALSE) {
# }
# 
