#' Conditioning Variable Division
#' 
#' Specify conditioning variable division parameters for data division
#'
#' @param vars a character string or vector of character strings specifying the variables of the input data across which to divide
#' 
#' @return a list to be used for the "by" argument to \code{\link{divide}}
#' 
#' @details Currently each unique combination of values of \code{vars} constitutes a subset.  In the future, specifying shingles for numeric conditioning variables will be implemented.
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{getSplitVars}}, \code{\link{getSplitVar}}
#' @export
condDiv <- function(vars) {
   # TODO: shingles, etc.
   res <- list(type="condDiv", vars=vars)
   class(res) <- c("condDiv", "divSpecList")
   res
}

#' @export
getCuts.condDiv <- function(by, curDF) {
   splitVars <- by$vars
   
   apply(curDF[,splitVars,drop=FALSE], 1, function(x) paste(paste(splitVars, "=", x, sep=""), collapse="|"))      
}

#' @export
validateDivSpec.condDiv <- function(by, data, ex) {
   if(by$type == "condDiv") {
      if(!all(by$vars %in% names(ex[[2]]))) {
         stop("'by' variables for conditioning division are not matched in data after applying preTransFn.  Try kvApply(preTransFn, kvExample(data, transform=TRUE)) to see what is expected.")
      }
   }

   NULL
}
