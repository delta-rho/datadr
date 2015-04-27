######################################################################
### ddo init methods
######################################################################

ddoInit <- function(obj, ...)
  UseMethod("ddoInit")

ddoInitConn <- function(obj, ...)
  UseMethod("ddoInitConn")

######################################################################
### requiredObjAttrs
######################################################################

requiredObjAttrs <- function(obj, ...)
  UseMethod("requiredObjAttrs")

######################################################################
### setAttributes
######################################################################

#' @rdname ddoddfattr
#' @export
setAttributes <- function(obj, attrs)
  UseMethod("setAttributes")

#' @rdname ddoddfattr
#' @param attrs a named list of attributes to set
#' @export
setAttributes.ddf <- function(obj, attrs) {
  ind <- which(names(attrs) %in% requiredObjAttrs(obj)$ddf)
  if(length(ind) > 0)
    obj <- setObjAttributes(obj, attrs[ind], type = "ddf")

  # all ddfs are also ddo's so call ddo directly instead of NextMethod
  setAttributes.ddo(obj, attrs[setdiff(seq_len(length(attrs)), ind)])
}

#' @rdname ddoddfattr
#' @export
setAttributes.ddo <- function(obj, attrs) {
  attrNames <- names(attrs)
  ind <- which(attrNames %in% requiredObjAttrs(obj)$ddo)

  if(length(ind) < length(attrNames))
    warning(paste("Unused attributes:", paste(attrNames[setdiff(1:length(attrNames), ind)], collapse = ", ")))

  if(length(ind) > 0)
    obj <- setObjAttributes(obj, attrs[ind], type = "ddo")

  obj
}

# setObjAttributes is called inside of setAttributes
# (once it has been determined whether it is a ddo or ddf attribute)
setObjAttributes <- function(obj, attrs, type) {
  attrNames <- names(attrs)

  if(is.null(attr(obj, type)))
    attr(obj, type) <- list()

  for(i in seq_along(attrs)) {
    attr(obj, type)[[attrNames[i]]] <- attrs[[i]]
  }

  saveAttrs(getAttribute(obj, "conn"), attr(obj, type), type = type)

  obj
}

######################################################################
### getAttributes
######################################################################

#' Managing attributes of 'ddo' or 'ddf' objects
#' @param obj 'ddo' or 'ddf' object
#' @param attrName name of the attribute to get
#' @rdname ddoddfattr
#' @export
getAttribute <- function(obj, attrName) {
  res <- getAttributes(obj, attrName)

  # getAttributes returns a list with "ddo" and "ddf"
  # the single attribute we want will be in the one of these that is not null
  if(length(res$ddf) == 0) {
    if(length(res$ddo) == 0) {
      return(NULL)
    } else {
      return(res$ddo[[1]])
    }
  } else {
    return(res$ddf[[1]])
  }
}

#' @rdname ddoddfattr
#' @param \ldots additional arguments
#' @export
getAttributes <- function(obj, attrNames)
  UseMethod("getAttributes")

#' @rdname ddoddfattr
#' @param attrNames vector of names of the attributes to get
#' @export
getAttributes.ddf <- function(obj, attrNames) {
  ind <- which(attrNames %in% requiredObjAttrs(obj)$ddf)
  res <- list(ddf = NULL)
  if(length(ind) > 0)
    res$ddf <- getObjAttributes(obj, attrNames[ind], type = "ddf")
  res <- c(res, getAttributes.ddo(obj, attrNames))
  res
}

# obj is the data object, attrs is a named list of attributes
#' @rdname ddoddfattr
#' @export
getAttributes.ddo <- function(obj, attrNames) {
  ind <- which(attrNames %in% requiredObjAttrs(obj)$ddo)
  res <- list(ddo = NULL)
  if(length(ind) > 0)
    res$ddo <- getObjAttributes(obj, attrNames[ind], type = "ddo")
  res
}

# setObjAttributes is called inside of setAttributes
# (once it has been determined whether it is a ddo or ddf attribute)
getObjAttributes <- function(obj, attrNames, type)
  attr(obj, type)[intersect(attrNames, names(attr(obj, type)))]

######################################################################
### hasAttributes
######################################################################

## returns a boolean vector the same length of the input vector of attribute names

#' @rdname ddoddfattr
#' @export
hasAttributes <- function(obj, ...)
  UseMethod("hasAttributes")

#' @rdname ddoddfattr
hasAttributes.ddf <- function(obj, attrNames) {
  res <- rep(FALSE, length(attrNames))

  ind <- which(attrNames %in% requiredObjAttrs(obj)$ddf)
  if(length(ind) > 0)
    res[ind] <- hasObjAttributes(obj, attrNames[ind], type = "ddf")

  res2 <- hasAttributes.ddo(obj, attrNames)
  res | res2
}

hasAttributes.ddo <- function(obj, attrNames) {
  res <- rep(FALSE, length(attrNames))

  ind <- which(attrNames %in% requiredObjAttrs(obj)$ddo)
  if(length(ind) > 0)
    res[ind] <- hasObjAttributes(obj, attrNames[ind], type = "ddo")

  res
}

hasObjAttributes <- function(obj, attrNames, type)
  attrNames %in% names(attr(obj, type))

getAttrNeedList <- function(obj, type) {
  rattrs <- requiredObjAttrs(obj)[[type]]
  attrs <- getAttributes(obj, rattrs)[[type]]
  if(!is.null(attrs)) {
    sapply(attrs, function(x) {
      ifelse(length(x) == 1 && !is.list(x) && !is.function(x), is.na(x), FALSE)
    })
  }
}

######################################################################
### extractor methods
######################################################################

#' @export
`[.ddo` <- function(x, i, ...) {
  # call extractor for whatever backend
  res <- extract(x, i, ...)

  # apply transformation functions
  transFns <- attr(x, "transforms")$transFns
  lapply(res, function(kv) {
    if(is.null(kv)) {
      return(NULL)
    } else {
      tmp <- applyTransform(transFns, kv)
      class(tmp) <- c("kvPair", "list")
      names(tmp) <- c("key", "value")
      tmp
    }
  })
}

#' @export
`[[.ddo` <- function(x, i, ...) {
  if(length(i) == 1) {
    res <- x[i]
    if(is.null(res)) {
      return(NULL)
    } else {
      return(res[[1]])
    }
  }
}

# backend-specific extraction method
extract <- function(x, ...)
  UseMethod("extract")

######################################################################
### other attribute methods
######################################################################

loadAttrs <- function(obj, ...)
  UseMethod("loadAttrs")

saveAttrs <- function(obj, ...)
  UseMethod("saveAttrs")

# "getBasic..." methods initialize the basic attributes
# (attributes that we can compute without running updateAttributes)

getBasicDdoAttrs <- function(obj, ...)
  UseMethod("getBasicDdoAttrs")

getBasicDdfAttrs <- function(obj, ...)
  UseMethod("getBasicDdfAttrs")

######################################################################
### special 'simplified' accessors
######################################################################

#' Accessor Functions
#'
#' Accessor functions for attributes of ddo/ddf objects.  Methods also include \code{nrow} and \code{ncol} for ddf objects.
#' @param x a 'ddf'/'ddo' object
#' @param object a 'ddf'/'ddo' object
#' @param \ldots additional arguments
#'
#' @export
#' @rdname ddo-ddf-accessors
kvExample <- function(x) {
  res <- getAttribute(x, "example")
  applyTransform(attr(x, "transforms")$transFns, res)
}

#' @export
#' @rdname ddo-ddf-accessors
bsvInfo <- function(x)
  getAttribute(x, "bsvInfo")

#' @export
#' @rdname ddo-ddf-accessors
counters <- function(x)
  getAttribute(x, "counters")

#' @export
#' @rdname ddo-ddf-accessors
splitSizeDistn <- function(x) {
  if(inherits(x, "transformed")) {
    message("The distribution of the size of each subset of a transformed divided data object is not accessible.")
    return(NA)
  }
  getAttribute(x, "splitSizeDistn")
}

#' @export
#' @rdname ddo-ddf-accessors
splitRowDistn <- function(x) {
  if(inherits(x, "transformed")) {
    message("The distribution of the number of rows in each subset of a transformed divided data frame is not accessible.")
    return(NA)
  }
  getAttribute(x, "splitRowDistn")
}

# need to change this in the future for k/v store with way too many keys
#' @export
#' @rdname ddo-ddf-accessors
getKeys <- function(x)
  getAttribute(x, "keys")

#' @rdname ddo-ddf-accessors
#' @method summary ddo
#' @export
summary.ddo <- function(object, ...) {
  if(inherits(object, "transformed")) {
    message("Summary statistics of a transformed divided data object are not accessible.")
    return(NA)
  }
  getAttribute(object, "summary")
}

#' @rdname ddo-ddf-accessors
#' @method summary ddf
#' @export
summary.ddf <- function(object, ...) {
  if(inherits(object, "transformed")) {
    message("Summary statistics of a transformed divided data frame are not accessible.")
    return(NA)
  }
  getAttribute(object, "summary")
}

#' @rdname ddo-ddf-accessors
#' @export
hasExtractableKV <- function(x)
  UseMethod("hasExtractableKV")

######################################################################
### this is for connections
######################################################################

#' Add Key-Value Pairs to a Data Connection
#'
#' Add key-value pairs to a data connection
#'
#' @param conn a kvConnection object
#' @param data a list of key-value pairs (list of lists where each sub-list has two elements, the key and the value)
#' @param overwrite if data with the same key is already present in the data, should it be overwritten? (does not work for HDFS connections)
#'
#' @author Ryan Hafen
#'
#' @note This is generally not recommended for HDFS as it writes a new file each time it is called, and can result in more individual files than Hadoop likes to deal with.
#' @seealso \code{\link{removeData}}, \code{\link{localDiskConn}}, \code{\link{hdfsConn}}
#'
#' @export
addData <- function(conn, data, overwrite = FALSE)
  UseMethod("addData")

#' Remove Key-Value Pairs from a Data Connection
#'
#' Remove key-value pairs from a data connection
#'
#' @param conn a kvConnection object
#' @param keys a list of keys indicating which k/v pairs to remove
#'
#' @author Ryan Hafen
#'
#' @note This is generally not recommended for HDFS as it writes a new file each time it is called, and can result in more individual files than Hadoop likes to deal with.
#' @seealso \code{\link{removeData}}, \code{\link{localDiskConn}}, \code{\link{hdfsConn}}
#'
#' @export
removeData <- function(conn, keys)
  UseMethod("removeData")

######################################################################
### object conversion
######################################################################

#' Convert 'ddo' / 'ddf' Objects
#'
#' Convert 'ddo' / 'ddf' objects between different storage backends
#'
#' @param from a 'ddo' or 'ddf' object
#' @param to a 'kvConnection' object (created with \code{\link{localDiskConn}} or \code{\link{hdfsConn}}) or \code{NULL} if an in-memory 'ddo' / 'ddf' is desired
#' @param overwrite should the data in the location pointed to in \code{to} be overwritten?
#' @export
convert <- function(from, to, overwrite = FALSE)
  UseMethod("convert")

# returns a list of classes it has been implemented for
# (used as a check before running a m/r job)
# probably a more elegant way to do this...
convertImplemented <- function(obj)
  UseMethod("convertImplemented")

# used by "convert" methods to add attributes - see if any are needed and if "from" has them
addNeededAttrs <- function(res, from) {
  ddoNeed <- getAttrNeedList(res, "ddo")
  ddoNeed <- names(ddoNeed[ddoNeed])
  ddfNeed <- getAttrNeedList(res, "ddf")
  ddfNeed <- names(ddfNeed[ddfNeed])

  fromAttrs <- list(
    ddo = getAttributes(from, getDr("requiredDdoAttrs"))$ddo,
    ddf = getAttributes(from, getDr("requiredDdfAttrs"))$ddf
  )

  newAttrs <- c(
    fromAttrs$ddo[names(fromAttrs$ddo) %in% ddoNeed],
    fromAttrs$ddf[names(fromAttrs$ddf) %in% ddfNeed]
  )

  setAttributes(res, newAttrs)
}

######################################################################
### nrow, ncol, names, length
######################################################################

setOldClass("ddf")
setOldClass("ddo")

#' 'ddf' accessors
#' @name ddf-class
#' @rdname ddf-class
#' @exportMethod nrow
setGeneric("nrow")
#' 'ddf' accessors
#' @name ddf-class
#' @rdname ddf-class
#' @exportMethod NROW
setGeneric("NROW")
#' 'ddf' accessors
#' @name ddf-class
#' @rdname ddf-class
#' @exportMethod ncol
setGeneric("ncol")
#' 'ddf' accessors
#' @name ddf-class
#' @rdname ddf-class
#' @exportMethod NCOL
setGeneric("NCOL")

#' The Number of Rows/Columns of a 'ddf' object
#' @rdname ddf-class
#' @param x a 'ddf' object
#' @export
#' @aliases nrow
setMethod("nrow", "ddf", function(x) {
  res <- getAttribute(x, "nRow")
  if(is.na(res) || is.null(res))
    warning("Number of rows has not been computed - run updateAttributes on this object")
  res
})

#' @rdname ddf-class
#' @export
#' @aliases NROW
setMethod("NROW", "ddf",  function(x) {
  res <- getAttribute(x, "nRow")
  if(is.na(res) || is.null(res))
    warning("Number of rows has not been computed - run updateAttributes on this object")
  res
})

#' @rdname ddf-class
#' @export
#' @aliases ncol
setMethod("ncol", "ddf", function(x) {
  length(attributes(x)$ddf$vars)
})

#' @rdname ddf-class
#' @export
#' @aliases NCOL
setMethod("NCOL", "ddf", function(x) {
  length(attributes(x)$ddf$vars)
})

# names and length are primitives

#' Accessor methods for 'ddo' and 'ddf' objects
#' @export
#' @method names ddf
#' @rdname ddo-ddf-accessors
names.ddf <- function(x) {
  if(inherits(x, "transformed")) {
    attributes(x)$transforms$varNames
  } else {
    names(attributes(x)$ddf$vars)
  }
}

#' @export
#' @method length ddo
#' @rdname ddo-ddf-accessors
length.ddo <- function(x) {
  getAttribute(x, "nDiv")
}

#' Turn 'ddf' Object into Data Frame
#'
#' Rbind all the rows of a 'ddf' object into a single data frame
#' @param x a 'ddf' object
#' @param row.names passed to \code{as.data.frame}
#' @param optional passed to \code{as.data.frame}
#' @param keys should the key be added as a variable in the resulting data frame? (if key is not a character, it will be replaced with a md5 hash)
#' @param splitVars should the values of the splitVars be added as variables in the resulting data frame?
#' @param bsvs should the values of bsvs be added as variables in the resulting data frame?
#' @param \ldots additional arguments passed to as.data.frame
#' @export
#' @method as.data.frame ddf
as.data.frame.ddf <- function(x, row.names = NULL, optional = FALSE, keys = TRUE, splitVars = TRUE, bsvs = FALSE, ...) {
  x <- convert(x, NULL)
  tmp <- lapply(getAttribute(x, "conn")$data, function(a) {
    res <- a[[2]]

    if(keys) {
      # TODO: what if 'key' already is a variable name?
      if(length(a[[1]] == 1)) {
        res$key <- a[[1]]
      } else {
        res$key <- digest(a[[1]])
      }
    }

    if(splitVars) {
      if(!is.null(getSplitVars(a)))
        res <- data.frame(res, getSplitVars(a))
    }

    if(bsvs) {
      if(!is.null(getBsvs(a)))
        res <- data.frame(res, getBsvs(a))
    }

    res
  })

  as.data.frame(rbindlist(tmp), row.names = row.names, optional = optional, ...)
}

#' Turn 'ddo' / 'ddf' Object into a list
#'
#' @param x a 'ddo' / 'ddf' object
#' @param \ldots additional arguments passed to \code{as.list}
#' @export
#' @method as.list ddo
as.list.ddo <- function(x, ...) {
  if(!inherits(x, "kvMemory"))
    x <- convert(x, NULL)
  getAttribute(x, "conn")$data
}
