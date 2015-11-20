#' Specify a Collection of Key-Value Pairs
#'
#' Specify a collection of key-value pairs
#'
#' @param \ldots key-value pairs (lists with two elements)
#'
#' @return a list of objects of class "kvPair"
#'
#' @seealso \code{\link{kvPair}}
#' @examples
#' kvPairs(kvPair(1, letters), kvPair(2, rnorm(10)))
#' @export
kvPairs <- function(...) {
  res <- list(...)
  for(i in seq_along(res)) {
    if(!inherits(res, "kvPair")) {
      if(length(res[[i]]) != 2)
        stop("Each key-value pair must be a list with two elements", call. = FALSE)
      names(res[[i]]) <- c("key", "value")
      class(res[[i]]) <- c("kvPair", "list")
    }
  }
  res
}


#' Specify a Key-Value Pair
#'
#' Specify a key-value pair
#'
#' @param k key - any R object
#' @param v value - any R object
#'
#' @return a list of objects of class "kvPair"
#'
#' @seealso \code{\link{kvPairs}}
#' @examples
#' kvPair("name", "bob")
#' @export
kvPair <- function(k, v)
  structure(list(key = k, value = v), class = c("kvPair", "list"))

#' Print a key-value pair
#'
#' @param x object to be printed
#' @param \ldots additional arguments
#' @export
#' @method print kvPair
print.kvPair <- function(x, ...) {
  if(!is.null(x[[2]]))
    class(x[[2]]) <- c("kvValue", class(x[[2]]))
  class(x) <- "list"
  print(x)
}

#' Print value of a key-value pair
#'
#' @param x object to be printed
#' @param \ldots additional arguments
#' @export
#' @method print kvValue
print.kvValue <- function(x, ...) {
  attr(x, "split") <- NULL
  class(x) <- setdiff(class(x), "kvValue")
  if(inherits(x, "data.frame")) {
    tmp <- head(x, 5)
    more <- NULL
    if(nrow(x) > 5)
      more <- "...\n"
    cat(paste(c(capture.output(tmp), more), sep = "", collapse = "\n"))
  } else {
    print(x)
  }
}
