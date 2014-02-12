#' Apply a function to all key/value pairs of a ddo/ddf object
#' 
#' This function creates a new ddo object by applying a function to all key/value pairs of a ddo/ddf object.
#'
#' @param X the ddo/ddf object to apply the function to
#' @param FUN the function to be applied to the value of each key/value pair
#' @return a new ddo object
#' @rdname lapply
#' @export
#' @examples
#' a <- divide(iris, by="Species")
#' b <- lapply(a, function(x) x$Sepal.Width)
setMethod("lapply", signature(X = "ddo", FUN = "function"), function(X, FUN) {
   if(length(formals(FUN)) > 1)
      stop("FUN must take only one argument for lapply on a ddo/ddf object")
   recombine(X, apply = FUN, combine=combDdo())
})

