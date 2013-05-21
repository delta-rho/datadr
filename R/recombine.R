#' Recombine a RHIPE 'rhDiv' Object
#' 
#' Apply an analytic method to a RHIPE 'rhDiv' object and combine the results
#' 
#' @param data an object of class 'rhDF'
#' @param apply the analytic method to apply to each subset
#' @param combine the method to combine the results
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{\link{rhwatch}} in RHIPE) - see \code{\link{rhipeControl}}
#' 
#' @return depends on \code{combine}
#' 
#' @details This is an experimental prototype - subject to change and be more flexible.
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{rrDiv}}, \code{\link{drGLM}}, \code{\link{wMeanCoef}}
#' @examples
#' \dontrun{
#' 
#' }
#' @export
recombine <- function(data=NULL, apply=NULL, combine=combCollect(), control=NULL) {
   
   divType <- getDivType(data)
   curSplit <- divExample(data)
   curKey <- divExampleKey(data)
   
   message("* Verifying suitability of 'apply' for division type...")
   
   if(!is.null(apply$validate)) {
      apply$validate(divType)
   }
   
   if(!is.null(combine$validate)) {
      combine$validate(divType)
   }
   
   message("* Testing the division method on a subset...")
   tmp <- apply$applyFn(c(apply$args, list(data=curSplit)), curKey)
   
   message("* Applying to all subsets...")
   
   map <- divApply(data, apply)
   res <- divCombine(data, map, apply, combine, control)
   
   combine$final(res, data)
}


