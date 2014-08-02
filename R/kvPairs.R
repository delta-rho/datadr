#' Manually Specify Key-Value Pairs
#' 
#' Manually specify key-value pairs
#' 
#' @param \ldots key-value pairs (lists with two elements)
#' 
#' @return a list of objects of class "kvPair"
#' 
#' @author Ryan Hafen
#' @export
kvPairs <- function(...) {
   res <- list(...)
   for(i in seq_along(res)) {
      if(length(res[[i]]) != 2)
         stop("Each key-value pair must be a list with two elements", call. = FALSE)
      names(res[[i]]) <- c("key", "value")
      class(res[[i]]) <- c("kvPair", "list")
   }
   res
}

#' Print a key-value pair
#' 
#' @param x object to be printed
#' @param \ldots additional arguments
#' @export
#' @method print kvPair
print.kvPair <- function(x, ...) {
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
