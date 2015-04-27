############################################################################
### ddo and ddf constructors
############################################################################

#' Instantiate a Distributed Data Frame ('ddf')
#'
#' Instantiate a distributed data frame ('ddf')
#'
#' @param conn an object pointing to where data is or will be stored for the 'ddf' object - can be a 'kvConnection' object created from \code{\link{localDiskConn}} or \code{\link{hdfsConn}}, or a data frame or list of key-value pairs
#' @param transFn transFn a function to be applied to the key-value pairs of this data prior to doing any processing, that transform the data into a data frame if it is not stored as such
#' @param update should the attributes of this object be updated?  See \code{\link{updateAttributes}} for more details.
#' @param reset should all persistent metadata about this object be removed and the object created from scratch?  This setting does not effect data stored in the connection location.
#' @param control parameters specifying how the backend should handle things if attributes are updated (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param verbose logical - print messages about what is being done
#' @export
ddf <- function(conn, transFn = NULL, update = FALSE, reset = FALSE, control = NULL, verbose = TRUE) {
  if(inherits(conn, "ddo")) {
    res <- conn
  } else {
    # call ddo "constructor"
    res <- ddo(conn, update = FALSE, reset = reset, control = control, verbose = verbose)
  }
  if(!inherits(res, "ddo"))
    stop("ddf() input must be a kvConnection or ddo object")
  class(res) <- c("ddf", class(res))

  if(!is.null(transFn)) {
    if(!is.null(transFn)) {
      message("** note **: transFn is deprecated - please apply this transformation using 'addTransform()' to your input data prior to calling 'divide()'")
      res <- addTransform(res, transFn)
    }
  }

  # make sure it is a data.frame
  ex <- kvExample(res)
  if(!inherits(ex[[2]], "data.frame")) {
    coerce <- try(as.data.frame(ex[[2]]), silent = TRUE)
    isCoercible <- ifelse(inherits(coerce, "try-error"), FALSE, TRUE)
    if(isCoercible) {
      message("*** data is not strictly a data frame, but coercible using as.data.frame - adding this transformation")
      res <- addTransform(res, function(x) as.data.frame(x))
    } else {
      stop("Data cannot be coerced to be a data frame")
    }
  }

  attrs <- loadAttrs(getAttribute(res, "conn"), type = "ddf")
  if(is.null(attrs) || reset) {
    # if(verbose)
      # message("* Getting basic 'ddf' attributes...")
    attrs <- getBasicDdfAttrs(res)
    attrs <- initAttrs(res, attrs, type = "ddf")
  } else {
    if(verbose)
      message("* Reading in existing 'ddf' attributes")
  }
  res <- setAttributes(res, attrs)

  # update attributes based on res
  if(update)
    res <- updateAttributes(res)

  res
}

#' Instantiate a Distributed Data Object ('ddo')
#'
#' Instantiate a distributed data object ('ddo')
#' @param conn an object pointing to where data is or will be stored for the 'ddf' object - can be a 'kvConnection' object created from \code{\link{localDiskConn}} or \code{\link{hdfsConn}}, or a data frame or list of key-value pairs
#' @param update should the attributes of this object be updated?  See \code{\link{updateAttributes}} for more details.
#' @param reset should all persistent metadata about this object be removed and the object created from scratch?  This setting does not effect data stored in the connection location.
#' @param control parameters specifying how the backend should handle things if attributes are updated (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param verbose logical - print messages about what is being done
#' @export
ddo <- function(conn, update = FALSE, reset = FALSE, control = NULL, verbose = TRUE) {
  # ddoInit should attach the conn attribute and add the ddo class to the object
  res <- ddoInit(conn)
  class(res) <- c("ddo", class(res))
  conn <- ddoInitConn(conn)

  attrs <- loadAttrs(conn, type = "ddo")
  if(length(attrs) == 0 || reset) {
    # if(verbose)
    #   message("* Getting basic 'ddo' attributes...")
    attrs <- getBasicDdoAttrs(res, conn)
    attrs <- initAttrs(res, attrs, type = "ddo")
  } else {
    if(verbose)
      message("* Reading in existing 'ddo' attributes")
  }
  res <- setAttributes(res, attrs)

  # update attributes based on conn
  if(update)
    res <- updateAttributes(res)

  res
}

############################################################################
### some helper functions
############################################################################

# fill a list with NAs with the desired attributes
initAttrs <- function(dat, attrList, type) {
  # initialize attributes
  rattrs <- requiredObjAttrs(dat)[[type]]
  # to start, set all to NA
  initAttrs <- rep(NA, length(rattrs))
  names(initAttrs) <- rattrs
  initAttrs <- as.list(initAttrs)
  # set attributes we can get without a M/R job

  initAttrs[names(attrList)] <- attrList
  initAttrs
}
