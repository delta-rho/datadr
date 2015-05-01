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
#'  \item \url{http://www.datadr.org}
#'  \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#'
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{getSplitVars}}, \code{\link{getSplitVar}}
#' @export
condDiv <- function(vars) {
  # TODO: shingles, etc.
  res <- list(type = "condDiv", vars = vars)
  class(res) <- c("condDiv", "divSpecList")
  res
}

#' @export
getCuts.condDiv <- function(by, curDF) {
  getCondCuts(curDF, by$vars)
}

#' Get names of the conditioning variable cuts
#'
#' Used internally for exported for certain reasons.  Do not use explicitly.
#' @param df a data frame
#' @param splitVars a vector of variable names to split by
#' @export
getCondCuts <- function(df, splitVars) {
  apply(do.call(cbind, lapply(df[,splitVars,drop = FALSE],
    function(x) format(x, scientific = FALSE, trim = TRUE, justify = "none"))), 1,
      function(x) paste(paste(splitVars, "=", x, sep = ""), collapse = "|"))
}

#' @export
validateDivSpec.condDiv <- function(by, data, ex) {
  if(by$type == "condDiv") {
    if(!all(by$vars %in% names(ex[[2]]))) {
      stop("'by' variables for conditioning division are not matched in data.  Look at a subset of the data, e.g. 'data[[1]]' to see what to expect.")
    }
  }

  NULL
}
