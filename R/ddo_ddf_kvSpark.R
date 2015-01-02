## Methods for object of class "kvSparkData" - key-value pairs as Spark RDDs

#' @export
ddoInit.sparkDataConn <- function(obj, ...) {
  structure(list(), class="kvSparkData")
}

#' @export
ddoInitConn.sparkDataConn <- function(obj, ...) {
  if(obj$hdfs) {
    paths <- paste(obj$hdfsURI, rhls(obj$loc, recurse = TRUE)$file, sep = "")
    regxp <- rhoptions()$file.types.remove.regex
  } else {
    paths <- paths[!grepl(rhoptions()$file.types.remove.regex, paths)]
    regxp <- "(/_meta|/_outputs|/_SUCCESS|/_LOG|/_log)"
  }
  paths <- paths[!grepl(regxp, paths)]
  if(length(paths) == 0)
    stop("No data found - use addData() or specify a connection with a location that contains data.")
  obj$data <- objectFile(getSparkContext(), paths)
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
  if(conn$hdfs) {
    ff <- rhls(conn$loc, recurse = TRUE)
    ff <- ff[!grepl("\\/_meta", ff$file),]
    sz <- sum(ff$size)
  } else {
    ff <- list.files(conn$loc, recursive = TRUE)
    ff <- ff[!grepl("_meta\\/", ff)]
    sz <- sum(file.info(file.path(fp, ff))$size)
  }

  ex <- take(conn$data, 1)[[1]]
  if(conn$type == "text")
    ex <- list("", ex)

  list(
    conn = conn,
    extractableKV = TRUE,
    totStorageSize = sz,
    totObjectSize = NA,
    nDiv = NA, # length(conn$data),
    example = ex
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
extract.kvSparkData <- function(x, i, ...) {
  idx <- NULL

  dat <- getAttribute(x, "conn")$data
  keys <- getKeys(x)

  if(is.numeric(i)) {
    idx <- i
    if(length(idx) == 1)
      return(take(dat, 1))
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

  lapply(idx, function(a) {
    lookup(dat, keys[[a]])
  })
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
  res <- collect(getAttribute(from, "conn")$data)

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
#   from
# }
#
# # from sparkData to HDFS
# #' @export
# convertkvSparkData.hdfsConn <- function(to, from, verbose=FALSE) {
# }
#
