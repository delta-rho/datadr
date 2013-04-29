#' GLM Recombination 'apply' Method
#' 
#' GLM recombination method
#' 
#' @param ldots arguments you would pass to the \code{\link{glm}} function
#' 
#' @details This is an experimental prototype.  It provides an function to be called for each subset in a recombination MapReduce job that applies R's glm method and outputs the coefficients.  It is to be passed as the argument \code{method} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#' 
#' @export
drGLM <- function(...) {
   args <- list(...)
   
   applyFn <- function(args, key) {
      fit <- do.call(glm, args)
      res <- list(
         names=names(coef(fit)),
         coef=as.numeric(coef(fit)),
         n=nrow(args$data)
      )
      class(res) <- c("drCoef", "list")
      list(key="1", val=res)      
   }
   
   list(args=args, applyFn=applyFn)
}

#' Generic 'apply' recombination method
drApply <- function(fn, by="") {
   list(
      args = list(applyFn=fn, byVal=by),
      applyFn = function(args, key) {
         res <- args$applyFn(args$data)
         attr(res, "split") <- attr(args$data, "split")
         if(is.null(args$byVal)) {
            key <- key
         } else if(args$byVal=="none") {
            if(is.data.frame(res)) {
               res$key <- key                     
            } else {
               res <- c(key, res)                     
            }
            key <- "1"
         }
         list(key=key, val=res)
      }
   )
}