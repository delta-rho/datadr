#' `[` Extract Method for 'localDiv'
#' 
#' `[` extrac method for 'localDiv'
#' 
#' @param x 'localDiv' object
#' @param ind index value
#' 
#' @details We want subsets of 'localDiv' to still behave like 'localDiv' objects.
#' 
#' @author Ryan Hafen
#' 
#' @export
"[.localDiv" <- function(x, ind) {
   class(x) <- "list"
   if(!is.numeric(ind))
      ind <- which(names(x) %in% ind)

   res <- x[ind]
   tmp <- attributes(x)
   tmp$names <- tmp$names[ind]
   attributes(res) <- tmp
   class(res) <- c("localDiv", "list")
   res
}

#' @export
"$.localDiv" <- function(x, val) {
   if(val %in% c("vars", "totSize", "ndiv", "trans", "nrow", "splitRowDistn", "divBy"))
      return(attr(x, val))
}
