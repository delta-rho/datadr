## Methods for object of class "kvMemory" - key-value pairs as R objects stored in memory

#' @export
ddoInit.data.frame <- function(obj, ...) {
  res <- list(list("", obj))
  ddoInit(res)
}

#' @export
ddoInitConn.data.frame <- function(obj, ...) {
  res <- list(list("", obj))
  ddoInitConn(res)
}

#' @export
ddoInit.list <- function(obj, ...) {
  structure(list(), class = "kvMemory")
}

#' @export
ddoInitConn.list <- function(obj, ...) {
  validateListKV(obj)
  structure(list(data = obj), class = c("nullConn", "kvConnection"))
}

#' @export
ddoInit.nullConn <- function(obj, ...) {
  structure(list(), class = "kvMemory")
}

#' @export
ddoInitConn.nullConn <- function(obj, ...) {
  obj
}

#' @export
requiredObjAttrs.kvMemory <- function(obj) {
  list(
    ddo = getDr("requiredDdoAttrs"),
    ddf = getDr("requiredDdfAttrs")
  )
}

#' @export
getBasicDdoAttrs.kvMemory <- function(obj, conn) {
  # data gets stored in conn, since conn is how things are passed around for other methods
  dat <- conn$data
  keys <- lapply(dat, "[[", 1)
  # names(keys) <- as.character(sapply(keys, digest))
  ts <- as.numeric(object.size(dat))
  list(
    conn = conn,
    keys = keys,
    keyHashes = sapply(keys, digest),
    extractableKV = TRUE,
    totStorageSize = ts,
    totObjectSize = ts,
    nDiv = length(dat),
    example = dat[[1]]
  )
}

#' @export
getBasicDdfAttrs.kvMemory <- function(obj) {
  list(
    nRow = sum(sapply(attr(obj, "ddo")$conn$data, function(x) nrow(x[[2]]))),
    vars = lapply(kvExample(obj)[[2]], class)
  )
}

# kvMemory is always extractable
#' @export
hasExtractableKV.kvMemory <- function(x) {
  TRUE
}

############################################################################
### extract methods
############################################################################

#' @export
extract.kvMemory <- function(x, i, ...) {
  idx <- NULL

  if(is.numeric(i)) {
    idx <- i
  } else {
    keyHashes <- getAttribute(x, "keyHashes")

    # try actual key
    idx <- unlist(lapply(as.character(sapply(i, digest)), function(x) which(keyHashes == x)))

    if(length(idx) == 0 && is.character(i)) {
      if(all(nchar(i) == 32)) {
        idx <- unlist(lapply(i, function(x) which(keyHashes == x)))
      }
    }
  }

  if(length(idx) == 0)
    return(NULL)
  getAttribute(x, "conn")$data[idx]
}

## default output methods (convert character to output)
##---------------------------------------------------------

charToOutput.kvMemoryChar <- function(x) {
  NULL
}

############################################################################
### convert methods
############################################################################

#' @export
convertImplemented.kvMemory <- function(obj) {
  c("localDiskConn", "hdfsConn", "NULL")
}

#' @export
convert.kvMemory <- function(from, to = NULL, overwrite = FALSE) {
  mrCheckOutputLoc(to, overwrite = overwrite)
  convertKvMemory(to, from)
}

convertKvMemory <- function(obj, ...)
  UseMethod("convertKvMemory", obj)

#' @export
convertKvMemory.NULL <- function(to, from, verbose = FALSE) {
  from
}

#' @export
convertKvMemory.localDiskConn <- function(to, from, verbose = FALSE) {
  # TODO: choose nBins based on nDiv, if it exists?
  addData(to, getAttribute(from, "conn")$data)

  if(inherits(from, "ddf")) {
    res <- ddf(to, update = FALSE, verbose = verbose)
  } else {
    res <- ddo(to, update = FALSE, verbose = verbose)
  }
  addNeededAttrs(res, from)
}

# go from memory to HDFS
#' @export
convertKvMemory.hdfsConn <- function(to, from, verbose = FALSE) {
  # strip out attributes
  writeDat <- getAttribute(from, "conn")$data
  attr(writeDat, "ddo") <- NULL
  attr(writeDat, "ddf") <- NULL
  class(writeDat) <- "list"
  rhwrite(writeDat, file = paste(to$loc, "/", digest(writeDat), "_", object.size(writeDat), sep = ""))

  if(inherits(from, "ddf")) {
    res <- ddf(to, update = FALSE, verbose = verbose)
  } else {
    res <- ddo(to, update = FALSE, verbose = verbose)
  }
  addNeededAttrs(res, from)
}

