
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
#' @seealso \code{\link{divide}}, \code{\link{rhDF}}, \code{\link{rrDiv}}
#' @examples
#' \dontrun{
#' 
#' }
#' @export
condDiv <- function(vars) {
   # TODO: shingles, etc.
   res <- list(type="condDiv", vars=vars)
   class(res) <- "divSpecList"
   res
}

#' Random Replicate Division
#'
#' Specify random replicate division parameters for data division
#'
#' @param nrows number of rows each subset should have
#' @param seed the random seed to use (experimental)
#' 
#' @return a list to be used for the "by" argument to \code{\link{divide}}
#' 
#' @details The random replicate division method currently gets the total number of rows of the data.frame and divides it by \code{nrows} to get the number of subsets.  Then it randomly assigns each row of the input data to one of the subsets, resulting in subsets with approximately \code{nrows} rows.  A future implementation will make each subset have exactly \code{nrows} rows.  
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }

#' 
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{rhDF}}, \code{\link{condDiv}}
#' @examples
#' \dontrun{
#' 
#' }
#' @export
rrDiv <- function(nrows=NULL, seed=NULL) {
   res <- list(type="rrDiv", nrows=nrows, seed=seed)
   class(res) <- "divSpecList"
   res
}