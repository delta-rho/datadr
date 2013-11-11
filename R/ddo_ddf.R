### distributed data frame attributes
# - vars (include place for factor levels, updated with updateAttributes)
# - transFn
# - badSchema
# - nRow
# - splitRowDistn
# - summary

### distributed data object attributes:
# - keys
# - loc
# - type
# - nfile
# - totSize
# - nDiv
# - splitSizeDistn
# - sourceconn
# - sourceJobconn
# - example
# - keySignature - a digest of the top 20 and bottom 20 digested keys plus number of subsets

# key signature is used to see if different data sets have the same division

## external methods for dealing with connections:
# addconn
# removeconn

## internal methods to implement for a new object:
# ddfInit
# ddoInit
# requiredObjAttrs: returns a vector of required attributes
# loadAttrs
# saveAttrs
# getBasicDdoAttrs
# getBasicDdfAttrs

## MapReduce methods
# mrExec
# defaultControl

############################################################################
### ddo and ddf constructors
############################################################################

# transFn is a function that turns each value into a data frame (if it isn't one)

# TODO: document
#' Make a distributed data frame
#' @param verbose logical - print messages about what is being done
#' @export
ddf <- function(conn, transFn=identity, update=FALSE, reset=FALSE, control=NULL, verbose=TRUE) {
   if(inherits(conn, "ddo")) {
      res <- conn
   } else {
      # call ddo "constructor"
      res <- ddo(conn, update=FALSE, reset=reset, control=control, verbose=verbose)
   }
   if(!inherits(res, "ddo"))
      stop("ddf() input must be a kvConnection or ddo object")
   class(res) <- c("ddf", class(res))      
   
   # make sure res can be a data frame
   transFn <- checkTransFn(kvExample(res), transFn)
   
   attrs <- loadAttrs(getAttribute(res, "conn"), type="ddf")
   if(is.null(attrs) || reset) {
      if(verbose)
         message("* Getting basic 'ddf' attributes...")
      attrs <- getBasicDdfAttrs(res, transFn)
      attrs <- initAttrs(res, attrs, type="ddf")
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

# TODO: document
#' Make a distributed data object
#' @param verbose logical - print messages about what is being done
#' @export
ddo <- function(conn, update=FALSE, reset=FALSE, control=NULL, verbose=TRUE) {
   # ddoInit should attach the conn attribute and add the ddo class to the object
   res <- ddoInit(conn)
   class(res) <- c("ddo", class(res))
   conn <- ddoInitConn(conn)
   
   attrs <- loadAttrs(conn, type="ddo")
   if(length(attrs) == 0 || reset) {
      if(verbose)
         message("* Getting basic 'ddo' attributes...")
      attrs <- getBasicDdoAttrs(res, conn)
      attrs <- initAttrs(res, attrs, type="ddo")
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

checkTransFn <- function(dat, transFn) {
   if(is.null(transFn))
      transFn <- identity
      
   isDF <- inherits(kvApply(transFn, dat), "data.frame")
   if(!isDF) {
      coerce <- try(as.data.frame(kvApply(transFn, dat)), silent=TRUE)
      isCoercible <- ifelse(inherits(coerce, "try-error"), FALSE, TRUE)
      # nested lists are coerced quite nicely:
      # as.data.frame(list(a=1:10, b=matrix(nrow=2, ncol=2), c=list(asdf=1, q=7, list(a="asdf")))) # this works
      # as.data.frame(list(var1=1:10, var2=1:3)) # this fails
      
      if(isCoercible) {
         warning("Data is not strictly a data frame, but coercible using as.data.frame")
         transFn <- as.data.frame
      } else {
         stop("Data cannot be coerced to be a data frame")
      }
   }
   transFn
}

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
