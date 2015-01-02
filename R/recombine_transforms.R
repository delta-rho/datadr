#' GLM Transformation Method
#'
#' GLM transformation method
#'
#' @param \ldots arguments you would pass to the \code{\link{glm}} function
#'
#' @details This provides a transformation function to be called for each subset in a recombination MapReduce job that applies R's glm method and outputs the coefficients in a way that \code{\link{combMeanCoef}} knows how to deal with.  It can be applied to a ddf with \code{\link{addTransform}} prior to calling \code{\link{recombine}}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#'
#' @export
drGLM <- function(...) {
  args <- list(...)
  fit <- do.call(glm, args)
  res <- list(
    names = names(coef(fit)),
    coef = as.numeric(coef(fit)),
    n = nrow(args$data)
  )
  class(res) <- c("drCoef", "list")
  res
}

#' Bag of Little Bootstraps Transformation Method
#'
#' Bag of little bootstraps transformation method
#'
#' @param x a subset of a ddf
#' @param statistic a function to apply to the subset specifying the statistic to compute.  Must have arguments 'data' and 'weights' - see details).  Must return a vector, where each element is a statistic of interest.
#' @param metric a function specifying the metric to be applied to the \code{R} bootstrap samples of each statistic returned by \code{statistic}.  Expects an input vector and should output a vector.
#' @param R the number of bootstrap samples
#' @param n the total number of observations in the data
#'
#' @details It is necessary to specify \code{weights} as a parameter to the \code{statistic} function because for BLB to work efficiently, it must resample each time with a sample of size \code{n}.  To make this computationally possible for very large \code{n}, we can use \code{weights} (see reference for details).  Therefore, only methods with a weights option can legitimately be used here.
#'
#' @references
#' BLB paper
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}
#'
#' @export
drBLB <- function(x, statistic, metric, R, n) {
  b <- nrow(x)
  resamples <- rmultinom(R, n, rep(1/b, b))

  res <- lapply(1:R, function(ii) {
    weights <- resamples[,ii] / max(resamples[,ii])
    suppressWarnings(statistic(x, weights))
  })
  res <- data.frame(do.call(rbind, res))

  as.numeric(do.call(c, lapply(res, metric)))
}


