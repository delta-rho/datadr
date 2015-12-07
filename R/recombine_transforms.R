#' GLM Transformation Method
#'
#' GLM transformation method -- Fit a generalized linear model to each subset
#'
#' @param \ldots arguments you would pass to the \code{\link{glm}} function
#'
#' @return An object of class \code{drCoef} that contains the glm coefficients and other data needed by \code{\link{combMeanCoef}}
#'
#' @details This provides a transformation function to be called for each subset in a recombination MapReduce job that applies R's glm method and outputs the coefficients in a way that \code{\link{combMeanCoef}} knows how to deal with.  It can be applied to a ddf with \code{\link{addTransform}} prior to calling \code{\link{recombine}}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#'
#' @examples
#' # Artificially dichotomize the Sepal.Lengths of the iris data to
#' # demonstrate a GLM model
#' irisD <- iris
#' irisD$Sepal <- as.numeric(irisD$Sepal.Length > median(irisD$Sepal.Length))
#'
#' # Divide the data
#' bySpecies <- divide(irisD, by = "Species")
#'
#' # A function to fit a logistic regression model to each species
#' logisticReg <- function(x)
#'   drGLM(Sepal ~ Sepal.Width + Petal.Length + Petal.Width,
#'         data = x, family = binomial())
#'
#' # Apply the transform and combine using 'combMeanCoef'
#' bySpecies %>%
#'   addTransform(logisticReg) %>%
#'   recombine(combMeanCoef)
#'
#' @export
drGLM <- function(...) {
  drM(..., type = "glm")
}

#' LM Transformation Method
#'
#' LM transformation method -- -- Fit a linear model to each subset
#'
#' @param \ldots arguments you would pass to the \code{\link{lm}} function
#'
#' @details This provides a transformation function to be called for each subset in a recombination MapReduce job that applies R's lm method and outputs the coefficients in a way that \code{\link{combMeanCoef}} knows how to deal with.  It can be applied to a ddf with \code{\link{addTransform}} prior to calling \code{\link{recombine}}.
#'
#' @return An object of class \code{drCoef} that contains the lm coefficients and other data needed by \code{\link{combMeanCoef}}
#'
#' @author Landon Sego
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#'
#' @examples
#' # Divide the data
#' bySpecies <- divide(iris, by = "Species")
#'
#' # A function to fit a multiple linear regression model to each species
#' linearReg <- function(x)
#'   drLM(Sepal.Length ~ Sepal.Width + Petal.Length + Petal.Width,
#'        data = x)
#'
#' # Apply the transform and combine using 'combMeanCoef'
#' bySpecies %>%
#'   addTransform(linearReg) %>%
#'   recombine(combMeanCoef)
#'
#' @export
drLM <- function(...) {
  drM(..., type = "lm")
}

# A generic, non-exported function for both glm and lm
drM <- function(..., type = c("lm", "glm")) {
  type <- match.arg(type)
  args <- list(...)
  fit <- do.call(type, args)
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
#' Kleiner, Ariel, et al. "A scalable bootstrap for massive data." Journal of the Royal Statistical Society: Series B (Statistical Methodology) 76.4 (2014): 795-816.
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


