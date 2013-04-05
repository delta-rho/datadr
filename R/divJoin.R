#' Join New Data to a "localDiv" object
#' 
#' Join new data to a "localDiv" object
#' 
#' @param obj an object of class "localDiv"
#' @param newDat new data / list / function with which to join to  \code{obj} (see details)
#' @param elemName the name of the new element created by the join for each split
#' 
#' @return the input object "obj" with the joined data
#'
#' @details \code{newDat} must be either a list of lists where the first element of each sublist matches those keys of localDiv, a data.frame with columns with the same names as splitVars, or a function that takes an element of localDiv as its argument and what it returns gets assigned to \code{elemName} of that split.
#' 
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}
#' 
#' @examples
#' irisSplit <- divide(iris, "Species")
#' 
#' # join a data.frame
#' newDatDF <- data.frame(
#'    Species=rep(unique(iris$Species), 3), 
#'    val=1:9
#' )
#' irisSplit <- divJoin(irisSplit, newDatDF, "testDF")
#' 
#' # join a list
#' newDatList <- lapply(paste("Species=", unique(iris$Species), sep=""), function(x) list(x, rnorm(5)))
#' irisSplit <- divJoin(irisSplit, newDatList, "testList")
#' 
#' # "join" using a function
#' newDatFun <- function(x) max(x$rows$Sepal.Length)
#' irisSplit <- divJoin(irisSplit, newDatFun, "testFun")
#'
#' @export
divJoin <- function(obj, newDat, elemName) {
   # TODO: make sure elemName is not "split" or "data"
   
   if(!inherits(obj, "localDiv"))
      stop("divJoin only works with objects of class 'localDiv'")
   
   divBy <- attr(obj, "divBy")
   if(divBy$type != "condDiv" && !inherits(newDat, "function"))
      stop("divJoin with newDat as a data.frame or list only works for conditional variable division")

   # if subsets are data.frames, we need a new data structure to contain the original rows of the data.frame plus the joined data
   subsetsAreDFs <- inherits(obj[[1]], "data.frame")

   newElem <- list()
   if(inherits(newDat, "function")) {
      newElem <- lapply(obj, function(x) {
         newDat(x)
      })
   } else {
      splitVars <- divBy$vars
   
      if(inherits(newDat, "data.frame")) {
         if(!all(splitVars %in% names(newDat)))
            stop("Supplied data.frame in argument 'newDat' does not have the correct columns for the conditioning variables that the data provided by 'obj' was partitioned by.")
      
         newSplitVars <- apply(newDat[,splitVars,drop=FALSE], 1, function(x) paste(splitVars, "=",x, collapse="|", sep=""))
         curSplitVars <- names(obj)
      
         splitIsPresent <- which(newSplitVars %in% curSplitVars)
         if(length(splitIsPresent) == 0) {
            warning("There were no matching splits.")         
         } else {
            newDat <- newDat[splitIsPresent, -which(splitVars %in% names(newDat)), drop=FALSE]
            uNewSplitVars <- unique(newSplitVars[splitIsPresent])
            for(x in uNewSplitVars) {
               curIdx <- which(curSplitVars == x)
               newIdx <- which(newSplitVars == x)
               newElem[[curIdx]] <- newDat[newIdx,, drop=FALSE]
            }
         }
      } else if(inherits(newDat, "list")) {
         newSplitVars <- sapply(newDat, function(x) x[[1]])
         curSplitVars <- names(obj)
         
         splitIsPresent <- which(newSplitVars %in% curSplitVars)
         newDat <- newDat[splitIsPresent]
         newSplitVars <- newSplitVars[splitIsPresent]
         splitVarIdx <- sapply(newSplitVars, function(x) which(curSplitVars == x))
         for(i in seq_along(newSplitVars)) {
            obj[[splitVarIdx[i]]][[elemName]] <- newDat[[i]][[2]]
         }
      }
   }
   if(length(newElem) > 0) {
      for(i in seq_along(obj)) {
         if(subsetsAreDFs) {
            obj[[i]] <- list(
               rows=obj[[i]],
               elemName=newElem[[i]]
            )
            attr(obj[[i]], "split") <- attr(obj[[i]]$rows, "split")
            attr(obj[[i]]$rows, "split") <- NULL
            names(obj[[i]]) <- c("rows", elemName)
         } else {
            obj[[i]][[elemName]] <- newElem[[i]]
         }
      }
   }
   
   
   obj
}