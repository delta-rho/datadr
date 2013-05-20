#' Get An Example Subset of Divided Data
#' Get a subset of a divided data object to test functions on prior to doing computation across the entire data set
#' 
#' @param divided data object (e.g. of class "localDiv", "rhDdata", etc.)
#' @param should the transformation function specified when the data was created be applied?
#'
#' @note \code{divExample(data)} returns the value from one key/value pair.  This is the form of the data when used in functions like \code{plotFn} or \code{cogFn} in \code{\link{makeDisplay}} or what it will look like when used in \code{\link{recombine}}.  If you want some sample key/value pairs to test some MapReduce code, simply use \code{data$example}.
#' 
#' @return one subset of the divided data
#'
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{rhipeDiv}}, \code{\link{makeDisplay}}
#'
#' @export
divExample <- function(data, ...) {
   UseMethod("divExample", data)
}

#' Get An Example Key of a Subset of Divided Data
#' Get the key for a subset of a divided data object to test functions on prior to doing computation across the entire data set
#' 
#' @param divided data object (e.g. of class "localDiv", "rhDdata", etc.)
#' @return one subset of the divided data
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{rhipeDiv}}, \code{\link{makeDisplay}}
#' @export
divExampleKey <- function(data) {
   UseMethod("divExampleKey", data)
}

#' Get Split Keys
#' Get split keys for divided data
#' @param obj divided data object (e.g. of class "localDiv", "rhData", etc.)
#' @author Ryan Hafen
#' @export
getKeys <- function(obj) {
   UseMethod("getKeys", obj)
}

getDivType <- function(data, ...) {
   UseMethod("getDivType", data)
}

getDivAttr <- function(obj) {
   UseMethod("getDivAttr", obj)
}

divApply <- function(obj, ...) {
   UseMethod("divApply", obj)   
}

divCombine <- function(obj, ...) {
   UseMethod("divCombine", obj)   
}
