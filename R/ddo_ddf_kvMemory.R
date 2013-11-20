## Methods for object of class "kvMemory" - key/value pairs as R objects stored in memory

#' @S3method ddoInit data.frame
ddoInit.data.frame <- function(obj, ...) {
   res <- list(list("", obj))
   ddoInit(res)
}

#' @S3method ddoInitConn data.frame
ddoInitConn.data.frame <- function(obj, ...) {
   res <- list(list("", obj))
   ddoInitConn(res)
}

#' @S3method ddoInit list
ddoInit.list <- function(obj, ...) {
   structure(list(), class="kvMemory")
}

#' @S3method ddoInitConn list
ddoInitConn.list <- function(obj, ...) {
   validateListKV(obj)
   structure(list(data=obj), class=c("nullConn", "kvConnection"))
}

#' @S3method ddoInit nullConn
ddoInit.nullConn <- function(obj, ...) {
   structure(list(), class="kvMemory")
}

#' @S3method ddoInitConn nullConn
ddoInitConn.nullConn <- function(obj, ...) {
   obj
}

#' @S3method requiredObjAttrs kvMemory
requiredObjAttrs.kvMemory <- function(obj) {
   list(
      ddo = getDr("requiredDdoAttrs"),
      ddf = getDr("requiredDdfAttrs")
   )
}

#' @S3method getBasicDdoAttrs kvMemory
getBasicDdoAttrs.kvMemory <- function(obj, conn) {
   # data gets stored in conn, since conn is how things are passed around for other methods
   dat <- conn$data
   keys <- lapply(dat, "[[", 1)
   names(keys) <- as.character(sapply(keys, digest))
   list(
      conn = conn,
      keys = keys,
      extractableKV = TRUE,
      totSize = as.numeric(object.size(dat)),
      nDiv = length(dat),
      example = dat[[1]]
   )
}

#' @S3method getBasicDdfAttrs kvMemory
getBasicDdfAttrs.kvMemory <- function(obj, transFn) {
   list(
      vars = lapply(getAttribute(obj, "conn")$data[[1]][[2]], class),
      transFn = transFn
   )
}

# kvMemory is always extractable
#' @S3method hasExtractableKV kvMemory
hasExtractableKV.kvMemory <- function(obj) {
   TRUE
}

############################################################################
### extract methods
############################################################################

#' @S3method [ kvMemory
`[.kvMemory` <- function(x, i, ...) {
   if(is.numeric(i)) {
      getAttribute(x, "conn")$data[i]
   } else {
      # if the key is most-likely a hash, try that
      idx1 <- NULL
      if(is.character(i)) {
         if(all(nchar(i) == 32)) {
            idx1 <- which(names(getKeys(x)) %in% i)
         }
      }
      idx2 <- which(names(getKeys(x)) %in% as.character(sapply(i, digest)))
      getAttribute(x, "conn")$data[union(idx1, idx2)]
   }
}

#' @S3method [[ kvMemory
`[[.kvMemory` <- function(x, i, ...) {
   if(length(i) == 1) {
      x[i][[1]]
   }
}

############################################################################
### convert methods
############################################################################

#' @S3method convertImplemented kvMemory
convertImplemented.kvMemory <- function(obj) {
   c("localDiskConn", "hdfsConn", "NULL")
}

#' @S3method convert kvMemory
convert.kvMemory <- function(from, to=NULL) {
   convertKvMemory(to, from)
}

convertKvMemory <- function(obj, ...)
   UseMethod("convertKvMemory", obj)

#' @S3method convertKvMemory NULL
convertKvMemory.NULL <- function(to, from, verbose=FALSE) {
   from
}

#' @S3method convertKvMemory localDiskConn
convertKvMemory.localDiskConn <- function(to, from, verbose=FALSE) {
   # make sure "to" is empty
   # TODO: choose nBins based on nDiv, if it exists?
   addData(to, getAttribute(from, "conn")$data)
   
   if(inherits(from, "ddf")) {
      res <- ddf(to, update=FALSE, verbose=verbose)
   } else {
      res <- ddo(to, update=FALSE, verbose=verbose)
   }
   addNeededAttrs(res, from)
}

# go from memory to HDFS
#' @S3method convertKvMemory hdfsConn
convertKvMemory.hdfsConn <- function(to, from, verbose=FALSE) {
   # strip out attributes
   writeDat <- getAttribute(from, "conn")$data
   attr(writeDat, "ddo") <- NULL
   attr(writeDat, "ddf") <- NULL
   class(writeDat) <- "list"
   rhwrite(writeDat, file=paste(to$loc, "/", digest(writeDat), "_", object.size(writeDat), sep=""))

   if(inherits(from, "ddf")) {
      res <- ddf(to, update=FALSE, verbose=verbose)
   } else {
      res <- ddo(to, update=FALSE, verbose=verbose)
   }
   addNeededAttrs(res, from)
}


