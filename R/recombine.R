#' Recombine
#' 
#' Apply an analytic recombination method to a ddo/ddf object and combine the results
#' 
#' @param data an object of class "ddo" of "ddf"
#' @param apply a function specifying the analytic method to apply to each subset, or a pre-defined apply function (see \code{\link{drBLB}}, \code{\link{drGLM}}, for example)
#' @param combine the method to combine the results
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' 
#' @return depends on \code{combine}
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{drGLM}}, \code{\link{drBLB}}, \code{\link{combMeanCoef}}, \code{\link{combMean}}, \code{\link{combCollect}}, \code{\link{combRbind}}
#' @export
recombine <- function(data, apply=NULL, combine=NULL, output=NULL, control=NULL, verbose=TRUE) {
   # apply <- function(x) {
   #    mean(x$Sepal.Length)
   # }
   # apply <- function(k, v) {
   #    list(mean(v$Sepal.Length)
   # }
   # apply <- drBLB(statistic=function(x, w) mean(x$Sepal.Length), metric=function(x) mean(x), R=100, n=300)
   # apply <- drGLM(Sepal.Length ~ Petal.Length)
   # combine <- combCollect()
   
   if(is.null(combine)) {
      if(is.null(output)) {
         combine <- combCollect()
      } else {
         combine <- combDdo()
      }
   }
   
   ## if apply is a function, build a map expression that applies it
   if(is.function(apply)) {
      apply <- structure(list(
         args = list(applyFn=apply),
         applyFn = function(args, dat) {
            kvApply(args$applyFn, dat)
         }
      ), class="drGeneric")
      environment(apply$applyFn) <- .GlobalEnv
   }
   
   if(verbose)
      message("* Verifying suitability of 'apply' for division type...")
   
   divInfo <- getAttribute(data, "div")
   divType <- ifelse(is.na(divInfo), "unknown", divInfo$divBy$type)
   if(!is.null(apply$validate))
      if(!divType %in% apply$validate)
         warning("* Input data with division: ", divType, " is not known to be suitable for the 'apply' method provided.  Interpret results at your own risk.")
   
   # if(verbose)
   #    message("* Verifying suitability of 'combine' for specified 'apply'...")
   
   if(verbose)
      message("* Verifying suitability of 'output' for specified 'combine'...")
   
   outClass <- ifelse(is.null(output), "nullConn", class(output)[1])
   if(!is.null(combine$validateOutput))
      if(!outClass %in% combine$validateOutput)
         stop("'output' of type ", outClass, " is not compatible with specified 'combine'")
   
   # group=TRUE says to output a constant key ("1"), so that everything
   # is bunched together in the reduce
   # group=FALSE says to output the same key that was input
   apply$args$group <- combine$group
   if(is.null(apply$args$group))
      apply$args$group <- TRUE
   
   if(!is.null(combine$mapHook)) {
      apply$args$mapHook <- combine$mapHook
      environment(apply$args$mapHook) <- .GlobalEnv
   }
   
   if(verbose)
      message("* Testing the 'apply' method on a subset...")
   tmp <- apply$applyFn(apply$args, kvExample(data))
   
   if(verbose)
      message("* Applying to all subsets...")
   map <- expression({
      for(i in seq_along(map.keys)) {
         tmp <- apply$applyFn(apply$args, list(map.keys[[i]], map.values[[i]]))
         if(apply$args$group) {
            key <- "1"
         } else {
            key <- map.keys[[i]]
         }
         if(is.function(apply$args$mapHook)) {
            tmp <- apply$args$mapHook(map.keys[[i]], tmp, attributes(map.values[[i]]))
         }
         collect(key, tmp)
      }
   })
   
   reduce <- combine$reduce
   
   parList <- list(
      apply = apply
   )
   
   if(! "package:datadr" %in% search()) {
      if(verbose)
         message("* ---- running dev version - sending datadr functions to mr job")
      parList <- c(parList, list(
         kvApply = kvApply
      ))
      
      setup <- expression({
         # suppressWarnings(suppressMessages(library(data.table)))
      })
   } else {
      setup <- expression({
         suppressWarnings(suppressMessages(library(datadr)))
      })
   }
   
   globalVars <- drFindGlobals(apply)
   globalVarList <- getGlobalVarList(globalVars, parent.frame())
   if(length(globalVarList) > 0)
      parList <- c(parList, globalVarList)
   
   res <- mrExec(
      data,
      output=output,
      setup=setup,
      map=map,
      reduce=reduce,
      params=parList,
      control=control
   )
   
   if(is.null(output)) {
      return(combine$final(res))
   } else {
      return(res)
   }
}


