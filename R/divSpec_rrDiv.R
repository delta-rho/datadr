#' Random Replicate Division
#'
#' Specify random replicate division parameters for data division
#'
#' @param nrows number of rows each subset should have
#' @param seed the random seed to use (experimental)
#'
#' @return a list to be used for the "by" argument to \code{\link{divide}}
#'
#' @details The random replicate division method currently gets the total number of rows of the input data and divides it by \code{nrows} to get the number of subsets.  Then it randomly assigns each row of the input data to one of the subsets, resulting in subsets with approximately \code{nrows} rows.  A future implementation will make each subset have exactly \code{nrows} rows.
#'
#' @references
#' \itemize{
#'  \item \url{http://tessera.io}
#'  \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#'
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{condDiv}}
#' @export
rrDiv <- function(nrows = NULL, seed = NULL) {
  res <- list(type = "rrDiv", nrows = nrows, seed = seed)
  class(res) <- c("rrDiv", "divSpecList")
  res
}

#' @export
getCuts.rrDiv <- function(by, curDF) {
  splitVars <- "__random__"
  # get the number of splits necessary for specified nrows
  ndiv <- by$specVars$ndiv
  n <- nrow(curDF)

  if(!is.null(by$seed)) set.seed(by$seed)
  # cuts <- paste("rr_", sample(1:ndiv, n, replace = TRUE), sep = "")
  paste("rr_", cut(stats::runif(n),
    seq(0, 1, length = ndiv + 1), labels = FALSE), sep = "")
}

#' @export
validateDivSpec.rrDiv <- function(by, data, ex) {
  n <- suppressWarnings(nrow(data))
  nr <- by$nrows
  ndiv <- round(n / nr, 0)
  if((is.null(n) || is.na(n)) && by$type == "rrDiv")
    stop("To do random replicate division, must know the total number of rows.  Call updateAttributes() on your data.")

  list(ndiv = ndiv, nrow = n)
}
