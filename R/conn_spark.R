# for now, SparkR data will just come from R objects
# HDFS support to come...

#' @S3method loadAttrs sparkDataConn
loadAttrs.sparkDataConn <- function(obj, type="ddo") {
   NULL
}

#' @S3method saveAttrs sparkDataConn
saveAttrs.sparkDataConn <- function(obj, attrs, type="ddo") {
   NULL
}

#' @S3method print sparkDataConn
print.sparkDataConn <- function(x, ...) {
   cat("Spark data connection (experimental)")
}

#' Connect to Spark Data Source
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








