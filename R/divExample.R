
#' Get An Example Subset of Divided Data
#'
#' Get a subset of an object of class "localDiv", "rhData" or "rhDF" to test test functions on prior to doing computation across the entire data set
#' 
#' @param data object of class "localDiv" or "rhDdata"
#' @param should the transformation function specified when the data was created be applied?
#'
#' @note \code{divExample(data)} returns the value from one key/value pair.  This is the form of the data when used in functions like \code{plotFn} or \code{cogFn} in \code{\link{vdbPlot}} or what it will look like when used in \code{\link{recombine}}.  If you want some sample key/value pairs to test some MapReduce code, simply use \code{data$example}.
#' 
#' @return one subset of the divided data
#'
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{rhipeDiv}}, \code{\link{vdbPlot}}
#'
#' @export
divExample <- function(data, trans=FALSE) {
   if(inherits(data, "localDiv")) {
      if(trans) {
         data <- attributes(data)$trans(data[[1]])
      } else {
         data <- data[[1]]
      }
   } else if(inherits(data, "rhData")) {
      if(trans) {
         data <- data$trans(data$example[[1]][[2]])
      }
      data <- data$example[[1]][[2]]
   }
   data
}

