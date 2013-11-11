#' GLM Recombination 'apply' Method
#' 
#' GLM recombination 'apply' method
#' 
#' @param ldots arguments you would pass to the \code{\link{glm}} function
#' 
#' @details This provides a function to be called for each subset in a recombination MapReduce job that applies R's glm method and outputs the coefficients.  It is to be passed as the argument \code{method} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#' 
#' @export
drGLM <- function(...) {
   args <- list(...)
   
   applyFn <- function(args, dat) {
      fit <- do.call(glm, c(args[!names(args)=="group"], list(data=dat[[2]])))
      res <- list(
         names=names(coef(fit)),
         coef=as.numeric(coef(fit)),
         n=nrow(dat[[2]])
      )
      class(res) <- c("drCoef", "list")
      res
   }
   
   structure(list(
      args=args, 
      applyFn=applyFn, 
      kvArgs=FALSE,
      validate=c("rrDiv", "condDiv", "unknown")
   ), class="drGLM")
}


#' Bag of Little Bootstraps Recombination 'apply' Method
#' 
#' Bag of little bootstraps recombination 'apply' method
#' 
#' @param statistic a function to apply to each subset specifying the statistic to compute.  Must have arguments 'data' and 'weights' - see details).  Must return a vector, where each element is a statistic of interest.
#' @param metric a function specifying the metric to be applied to the \code{R} bootstrap samples of each statistic returned by \code{statistic}.  Expects an input vector and should output a vector.
#' @param R the number of bootstrap samples
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
drBLB <- function(statistic, metric, R, n) {
   args <- list(statistic=statistic, metric=metric, R=R, n=n)
   
   applyFn <- function(args, dat) {
      b <- nrow(dat[[2]])
      
      resamples <- rmultinom(args$R, args$n, rep(1/b, b))
      
      res <- lapply(1:args$R, function(ii) {
         weights <- resamples[,ii] / max(resamples[,ii])
         suppressWarnings(args$statistic(dat[[2]], weights))
      })
      res <- data.frame(do.call(rbind, res))
      
      as.numeric(do.call(c, lapply(res, args$metric)))
   }
   
   structure(list(
      args=args, 
      applyFn=applyFn, 
      kvArgs=FALSE,
      validate="rrDiv"
   ), class="drBLB")
}

