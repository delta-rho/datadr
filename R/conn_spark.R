# for now, SparkR data will just come from R objects
# HDFS support to come...

#' @export
loadAttrs.sparkDataConn <- function(obj, type="ddo") {
   NULL
}

#' @export
saveAttrs.sparkDataConn <- function(obj, attrs, type="ddo") {
   NULL
}

#' @export
print.sparkDataConn <- function(x, ...) {
   cat("Spark data connection (experimental)")
}

#' Connect to Spark Data Source
#' 
#' Connect to Spark data source (experimental).
#' 
#' @param data a data frame or list of key-value pairs
#' @param init a named list of arguments to be passed to \code{sparkR.init}
#' @param verbose logical - print messages about what is being done
#' 
#' @note This is currently a proof-of-concept.  It only allows in-memory data to be initialized as a Spark RDD, which is quite pointless for big data.  In the future, this will allow connections to link to data on HDFS.
#' 
#' @export
sparkDataConn <- function(data = NULL, init = list(), verbose = TRUE) {
   require(SparkR)
   
   # this is a proof-of-concept now
   # we simply store the data and parallelize when needed
   if(inherits(data, "data.frame"))
      data <- list(list("", data))
   
   conn <- list(data = data, init = init)
   class(conn) <- c("sparkDataConn", "kvConnection")
   
   conn
}

mrCheckOutputLoc.sparkDataConn <- function(x)
   NULL






