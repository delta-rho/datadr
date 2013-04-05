#' Recombine a RHIPE 'rhDiv' Object
#' 
#' Apply an analytic method to a RHIPE 'rhDiv' object and combine the results
#' 
#' @param data an object of class 'rhDF'
#' @param method the analytic method to apply to each subset
#' @param recomb the method to combine output from the analytic method on each subset
#' @param mapred mapreduce-specific parameters to send to RHIPE job that performs the recombination (see \code{\link{rhwatch}})
#' 
#' @return depends on \code{recomb} - should be a final set of model coefficients, etc.
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
recombine <- function(data=NULL, method=NULL, recomb=NULL, mapred=NULL) {
   
   if(inherits(data, "rhDiv")) {
      divType <- data$divBy$type
      curSplit <- divExample(data)

      message("*** Testing the division method on a subset...")
      tmp <- method$expr(c(method$args, list(data=curSplit)))

      map <- rhmap({
         curSplit <- r
         tmp <- method$expr(c(method$args, list(data=curSplit)))
         rhcollect(tmp$key, tmp$val)
      })

      reduce <- recomb

      res <- rhwatch(
         input=rhfmt(data$loc, type=data$type),
         map=map,
         reduce=reduce,
         mapred=mapred,
         parameters=list(method=method)
      )

      return(res[[1]][[2]])
   } else if(inherits(data, "localDiv")) {
      divType <- attr(data, "divBy")$type
      curSplit <- divExample(data)

      message("*** Testing the division method on a subset...")
      tmp <- method$expr(c(method$args, list(data=curSplit)))

      map <- lapply(data, function(curSplit) {
         method$expr(c(method$args, list(data=curSplit)))         
      })
      
      keys <- sapply(map, function(x) x$key)
      uKeys <- unique(keys)
      
      res <- lapply(uKeys, function(x) {
         reduce.key <- x
         reduce.values <- lapply(map[keys==x], function(a) a$val)
         rhcollect <- function(...) list(...)
         eval(recomb$pre)
         eval(recomb$reduce)
         eval(recomb$post)
      })

      return(res[[1]][[2]])
   }

}

#' GLM Recombination Method
#' 
#' GLM recombination method
#' 
#' @param ldots arguments you would pass to the \code{\link{glm}} function
#' 
#' @details This is an experimental prototype.  It provides an expression to be evaluated for each subset in a recombination MapReduce job that applies R's glm method and outputs the coefficients.  It is to be passed as the argument \code{method} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#' 
#' @export
drGLM <- function(...) {
   args <- list(...)

   expr <- function(args) {
      fit <- do.call(glm, args)
      res <- list(
         names=names(coef(fit)),
         coef=as.numeric(coef(fit)),
         n=nrow(args$data)
      )
      class(res) <- c("drCoef", "list")
      list(key="1", val=res)      
   }

   list(args=args, expr=expr)
}

#' Weighted Coefficient Mean Recombination
#' 
#' Weighted coefficient mean recombination
#' 
#' @details This is an experimental prototype.  It is to be passed as the argument \code{recomb} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#' 
#' @export
wMeanCoef <- function() {
   expression(
      pre = {
         res <- NULL
         n <- as.numeric(0)
         coefNames <- NULL
      },
      reduce = {
         if(is.null(coefNames))
            coefNames <- reduce.values[[1]]$names
            
         n <- sum(c(n, unlist(lapply(reduce.values, function(x) x$n))), na.rm=TRUE)
         res <- do.call(rbind, c(list(res), lapply(reduce.values, function(x) {
            x$coef * x$n
         })))
         res <- apply(res, 2, sum)
      },
      post = {
         res <- res / n
         names(res) <- coefNames
         rhcollect("final", res)
      }
   )
}
