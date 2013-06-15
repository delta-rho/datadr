#' Mean Coefficient Recombination
#' 
#' Mean coefficient recombination
#' 
#' @param \ldots ...
#'
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.  It expects to be dealing with named vectors including an element \code{n} specifying the number of rows in that subset.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}
#' 
#' @export
combMeanCoef <- function(...) {
   list(
      reduce=expression(
         pre = {
            suppressWarnings(suppressMessages(require(data.table)))
            res <- NULL
            n <- as.numeric(0)
            coefNames <- NULL
         },
         reduce = {
            if(is.null(coefNames))
               coefNames <- reduce.values[[1]]$names
               
            n <- sum(c(n, unlist(lapply(reduce.values, function(x) x$n))), na.rm=TRUE)
            res <- rbindlist(c(list(res), lapply(reduce.values, function(x) {
               x$coef * x$n
            })))
            res <- apply(res, 2, sum)
         },
         post = {
            res <- res / n
            names(res) <- coefNames
            rhcollect("final", res)
         }
      ),
      final=function(x, ...) x[[1]][[2]],
      readback=TRUE,
      validate=function(divType) {
         if(divType != "rrDiv")
            warning("The combMeanCoef() combine method is designed to be used with data random replicate division data.  Interpret the results at your own risk.")
      },
      ...
   )
}

#' Mean Recombination
#' 
#' Mean recombination
#' 
#' @param \ldots ...
#' 
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}
#' 
#' @export
combMean <- function(...) {
   list(
      reduce=expression(
         pre = {
            suppressWarnings(suppressMessages(require(data.table)))
            res <- NULL
            n <- as.numeric(0)
         },
         reduce = {
            n <- sum(c(n, length(reduce.values)))
            res <- rbindlist(c(list(res), lapply(reduce.values, function(x) {
               x
            })))
            res <- apply(res, 2, sum)
         },
         post = {
            res <- res / n
            rhcollect("final", res)
         }
      ),
      final=function(x, ...) x[[1]][[2]],
      readback=TRUE,
      ...
   )
}

#' "Collect" Recombination
#' 
#' "Collect" recombination
#' 
#' @param \ldots ...
#' 
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}
#' 
#' @export
combCollect <- function(...) {
   list(
      reduce=expression(reduce = {
         lapply(reduce.values, function(r) rhcollect(reduce.key, r))
      }), # this is from rhoptions()$template$identity (but user might not have RHIPE installed)
      final=function(x, ...) x,
      readback=TRUE,
      ...
   )
}

#' "rbind" Recombination
#' 
#' "rbind" recombination
#' 
#' @param \ldots ...
#' 
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}
#' 
#' @export
combRbind <- function(...) {
   red <- expression(
      pre = {
         adata <- list()
      }, 
      reduce = {
         adata[[length(adata) + 1]] <- reduce.values
      }, 
      post = {
         adata <- data.frame(rbindlist(unlist(adata, recursive=FALSE)))
         rhcollect(reduce.key, adata)
      }
   )
   attr(red, "combine") <- TRUE

   list(
      reduce=red,
      final=function(x, ...) {
         if(length(x) == 1) {
            return(x[[1]][[2]])
         } else {
            nms <- sapply(x, "[[", 1)
            res <- lapply(x, "[[", 2)
            names(res) <- nms
            res
         }
      },
      readback=TRUE,
      ...
      # TODO: should make sure the result won't be too big (approximate by size of output from test run times number of divisions)
   )
}

#' Division Recombination
#' 
#' Division recombination (output of recombine is an object of class localDiv or rhDiv)
#' 
#' @param \ldots ...
#' 
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divide}}, \code{\link{recombine}}
#' 
#' @export
combDiv <- function(...) {
   list(
      reduce=expression(reduce = {
         lapply(reduce.values, function(r) rhcollect(reduce.key, r))
      }), # from rhoptions()$template$identity
      inputData=function(data) {
         data
      },
      final=function(res, data) {
         combDivFinal(data, res)
      },
      readback=FALSE,
      copyDivAttr=TRUE,
      ...
   )
}

combDivFinal <- function(obj, ...) {
   UseMethod("combDivFinal", obj)
}

combDivFinal.localDiv <- function(data, x) {
   keys <- sapply(x, "[[", 1)
   res <- lapply(x, "[[", 2)
   names(res) <- keys
   class(res) <- c("localDiv", "list")
   
   attr(res, "vars") <- lapply(res[[1]], class)
   attr(res, "totSize") <- as.numeric(object.size(res))
   attr(res, "ndiv") <- length(res)
   attr(res, "trans") <- attr(data, "trans")
   attr(res, "nrow") <- sum(sapply(res, nrow))
   attr(res, "splitRowDistn") <- quantile(sapply(res, nrow), probs=seq(0, 1, by=0.0001))
   attr(res, "divBy") <- attr(data, "divBy")
   res
}

combDivFinal.rhDiv <- function(data, x) {
   # TODO: validate - this only works when readback is FALSE
   loc <- x[[2]]$lines$rhipe_output_folder

   divAttr <- loadRhDivAttr(data$loc)
   saveRhDivAttr(divAttr, loc=loc)

   res <- rhDF(loc, type="map")
   res
}


