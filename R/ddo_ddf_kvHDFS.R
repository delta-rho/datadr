## Methods for object of class "kvHDFS" - key-value pairs as R objects stored on HDFS

#' @export
ddoInit.hdfsConn <- function(obj, ...) {
  structure(list(), class = "kvHDFS")
}

#' @export
ddoInitConn.hdfsConn <- function(obj, ...) {
  obj
}

#' @export
requiredObjAttrs.kvHDFS <- function(obj) {
  list(
    ddo = c(getDr("requiredDdoAttrs"), "nFile", "sourceData", "sourceJobData"),
    ddf = getDr("requiredDdfAttrs")
  )
}

#' @export
getBasicDdoAttrs.kvHDFS <- function(obj, conn) {
  fp <- conn$loc
  tp <- conn$type
  ff <- rhls(fp, recurse = TRUE)
  ff <- ff[!grepl("\\/_meta", ff$file),]
  if(nrow(ff) == 0)
    stop("No data", call. = FALSE)
  sz <- ff$size
  tmp <- rhread(ff$file, type = tp, max = 1)[[1]]
  if(tp == "text")
    tmp <- list("", tmp)

  list(
    conn = conn,
    extractableKV = FALSE,
    totStorageSize = sum(sz),
    nFile = length(ff$file),
    example = tmp
  )
}

#' @export
getBasicDdfAttrs.kvHDFS <- function(obj) {
  list(
    vars = lapply(kvExample(obj)[[2]], class)
  )
}

#' @export
hasExtractableKV.kvHDFS <- function(x) {
  # grab one key and see if you can get it with rhmapfile
  conn <- getAttribute(x, "conn")
  k <- kvExample(x)[1]
  err <- try(a <- rhmapfile(conn$loc), silent = TRUE)
  if(inherits(err, "try-error")) {
    return(FALSE)
  } else {
    err <- try(tmp <- a[k], silent = TRUE)
    if(inherits(err, "try-error")) {
      return(FALSE)
    } else {
      return(!is.null(tmp))
    }
  }
}

############################################################################
###
############################################################################

#' Take a ddo/ddf HDFS data object and turn it into a mapfile
#' @param obj object of class 'ddo' or 'ddf' with an HDFS connection
#' @export
makeExtractable <- function(obj) {
  if(!inherits(obj, "kvHDFS"))
    stop("object must have an HDFS connection")

  mkd <- utils::getFromNamespace("mkdHDFSTempFolder", "Rhipe")

  # identity mr job
  res <- mrExec(
    obj,
    output = hdfsConn(mkd(file = "tmp_output"), type = "map", autoYes = TRUE, verbose = FALSE)
  )

  # now move temporary over to original
  resConn <- attr(res, "ddo")$conn
  objConn <- attr(obj, "ddo")$conn

  # delete all but meta
  ff <- rhls(objConn$loc)$file
  ff <- ff[!grepl("_meta$", ff)]
  for(f in ff)
    rhdel(f)

  # now move over all but meta
  ff <- rhls(resConn$loc)$file
  ff <- ff[!grepl("_meta$", ff)]
  ff <- ff[!grepl("_meta$", ff)]
  for(f in ff)
    rhmv(f, objConn$loc)

  if(inherits(obj, "ddf")) {
    res <- ddf(objConn, update = FALSE, verbose = FALSE)
  } else {
    res <- ddo(objConn, update = FALSE, verbose = FALSE)
  }

  objConn$type <- "map"
  res <- setAttributes(res, list(conn = objConn, extractableKV = TRUE))

  res
}

############################################################################
### extract methods
############################################################################

#' @export
extract.kvHDFS <- function(x, i, ...) {
  conn <- getAttribute(x, "conn")
  if(is.numeric(i)) {
    if(i == 1 || !hasAttributes(x, "keys")) {
      if(length(i) > 1)
      message("Keys are not known - just retrieving the first ", length(i), " key-value pair(s).")
      res <- rhread(rhls(conn$loc, recurse = TRUE)$file, type = conn$type, max = length(i))
    } else {
      if(!hasExtractableKV(x))
        stop("This data must not be a valid mapfile -- cannot extract subsets by key.  Call makeExtractable() on this data.")
      a <- rhmapfile(getAttribute(x, "conn")$loc)
      keys <- getKeys(x)[i]
      res <- a[keys]
      res <- lapply(seq_along(keys), function(a) list(keys[[a]], res[[a]]))
    }
  } else {
    if(!hasExtractableKV(x))
      stop("This data must not be a valid mapfile -- cannot extract subsets by key.  Call makeExtractable() on this data.")
    a <- rhmapfile(getAttribute(x, "conn")$loc)
    # if the object does not have keys attribute,
    # the best we can do is try to try to use "i" as-is,
    # as we can't try to do a lookup

    if(!hasAttributes(x, "keys")) {
      res <- a[i]
      keys <- i
    } else {
      # if the key is most-likely a hash, try that
      res <- NULL
      if(is.character(i)) {
        if(all(nchar(i) == 32)) {
          kh <- getAttribute(x, "keyHashes")
          idx <- unlist(lapply(i, function(x) which(kh == x)))
          keys <- getKeys(x)[idx]
          res <- a[keys]
        }
      }
      if(is.null(res)) {
        keys <- i
        res <- a[i]
      }
    }
    res <- lapply(seq_along(keys), function(a) list(keys[[a]], res[[a]]))
  }
  if(conn$type == "text")
    res <- lapply(res, function(x) list("", x))
  res
}

## default output methods (convert character to output)
##---------------------------------------------------------

charToOutput.kvHDFSChar <- function(x) {
  hdfsConn(x)
}

############################################################################
### convert methods
############################################################################

#' @export
convertImplemented.kvHDFS <- function(obj)
  c("localDiskConn", "NULL", "hdfsConn")

#' @export
convert.kvHDFS <- function(from, to = NULL, overwrite = FALSE) {
  if(!inherits(to, "hdfsConn"))
    mrCheckOutputLoc(to, overwrite = overwrite)
  convertKvHDFS(to, from, overwrite)
}

convertKvHDFS <- function(obj, ...)
  UseMethod("convertKvHDFS", obj)

#' @export
convertKvHDFS.hdfsConn <- function(to, from, verbose = FALSE)
  from

#' @export
convertKvHDFS.localDiskConn <- function(to, from, verbose = FALSE) {
  # convert from kvHDFS to kvLocalDisk (from = kvHDFS, to = localDiskConn)

  conn <- getAttribute(from, "conn")
  a <- rhIterator(rhls(conn$loc, recurse = TRUE)$file, type = conn$type, chunksize = 50*1024^2, chunk = "bytes")

  if(verbose)
    message("* Moving HDFS k/v pairs to local disk")
  while(length(b <- a()) > 0) {
    addData(to, b)
  }

  if(inherits(from, "ddf")) {
    res <- ddf(to, update = FALSE, verbose = verbose)
  } else {
    res <- ddo(to, update = FALSE, verbose = verbose)
  }

  addNeededAttrs(res, from)
}

#' @export
convertKvHDFS.NULL <- function(to, from, verbose = FALSE) {
  size <- getAttribute(from, "totObjectSize")
  if(is.na(size))
    size <- getAttribute(from, "totStorageSize")
  if(size / 1024^2 > 100)
    warning("Reading over 100MB of data into memory - probably not a good idea...")

  fromConn <- attr(from, "ddo")$conn
  res <- rhread(rhls(fromConn$loc, recurse = TRUE)$file, type = fromConn$type)

  if(inherits(from, "ddf")) {
    res <- ddf(res, update = FALSE, verbose = verbose)
  } else {
    res <- ddo(res, update = FALSE, verbose = verbose)
  }

  addNeededAttrs(res, from)
}


