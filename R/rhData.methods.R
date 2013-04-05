#' str Method for 'rhData'
#'
#' str method for 'rhData'
#'
#' @param x 'rhData' object
#' @param \ldots additional parameters
#' 
#' @details Avoids clutter with \code{\link{str}}, hiding \code{sourceJobData} and \code{example}, whose structure can be seen with, e.g. \code{str(obj$example)}
#' 
#' @author Ryan Hafen
#' 
#' @export
str.rhData <- function(x, ...) {
   if(!all(is.na(x$sourceJobData)))
      x$sourceJobData <- "hidden list"

   if(!all(is.na(x$example)))
      x$example <- "hidden list"

   utils:::str.default(x, ...)
}

#' `[[` Extract Method for 'rhData'
#' 
#' `[[` extrac method for 'rhData'
#' 
#' @param x 'rhData' object
#' @param ind index value
#' 
#' @details By default, we want obj[[key]] to extract a key/value pair from 'rhData' objects, but still want to extract metadata if specified by name.
#' 
#' @author Ryan Hafen
#' 
#' @export
"[[.rhData" <- function(x, ind) {
   # preserve subsetting if it isn't a key
   if(ind %in% c("vars", "isDF", "badSchema", "nrow", "splitRowDistn", "summary", "hasKeys", "loc", "type", "nfile", "totSize", "ndiv", "splitSizeDistn", "sourceData", "sourceJobData", "trans", "mapfile", "example", "divBy") || is.numeric(ind)) {
      class(x) <- "list"
      return(x[[ind]])
   }
   
   if(x$type != "map")
      stop("Subset operation only works if data is in map format.")
   
   if(suppressWarnings(is.na(x$mapfile)) || is.null(x$mapfile)) {
      warning("mapfile for subsetting was not available - re-initializing...")
      x$mapfile <- rhmapfile(rhDataAttr$loc)
   }
   x$mapfile[[ind]]
}

#' `[` Extract Method for 'rhData'
#' 
#' `[` extrac method for 'rhData'
#' 
#' @param x 'rhData' object
#' @param ind index value
#' 
#' @details By default, we want obj[keys] to extract key/value pairs from 'rhData' objects, but still want to extract metadata if specified by name.
#' 
#' @author Ryan Hafen
#' 
#' @export
"[.rhData" <- function(x, ind) {
   # preserve subsetting if it isn't a key
   if(ind %in% c("vars", "isDF", "badSchema", "nrow", "splitRowDistn", "summary", "hasKeys", "loc", "type", "nfile", "totSize", "ndiv", "splitSizeDistn", "sourceData", "sourceJobData", "trans", "mapfile", "example", "divBy") || is.numeric(ind)) {
      class(x) <- "list"
      return(x[ind])
   }

   if(x$type != "map")
      stop("Subset operation only works if data is in map format.")
   
   if(suppressWarnings(is.na(x$mapfile)) || is.null(x$mapfile)) {
      warning("mapfile for subsetting was not available - re-initializing...")
      x$mapfile <- rhmapfile(rhDataAttr$loc)
   }
   x$mapfile[ind]
}