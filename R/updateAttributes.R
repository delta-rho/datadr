#' Update Attributes of a 'rhData' or 'rhDF' Object
#'
#' Update attributes of a 'rhData' or 'rhDF' object
#' 
#' @param obj an object of class 'rhData' or 'rhDF'
#' @param mapred mapreduce-specific parameters to send to RHIPE job that creates the division (see \code{\link{rhwatch}})
#' 
#' @return an object of class 'rhData' or 'rhDF'
#' 
#' @details This function looks for missing attributes related to a RHIPE data object and runs a MapReduce job to update them.  These attributes include "splitSizeDistn", "hasKeys", "ndiv", "nrow", and "splitRowDistn".  These attributes are useful for subsequent computations that might rely on them.  The result is the input modified to reflect the updated attributes, and thus it should be used as \code{obj <- updateAttributes(obj)}.  Metadata files on HDFS are also updated.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{rhData}}, \code{\link{rhDF}}, \code{\link{divide.rhData}}
#'
#' @examples
#' \dontrun{
#' 
#' }
#' @export
updateAttributes <- function(obj, mapred=NULL) {
   
   rhDataVars <- c("hasKeys", "ndiv", "splitSizeDistn")
   rhDFvars <- c("nrow", "splitRowDistn", "summary", "badSchema")
   
   updateableVars <- c(rhDataVars, rhDFvars)
   
   needList <- sapply(updateableVars, function(a) {
      ifelse(is.null(obj[[a]]) || is.na(obj[[a]]) || !obj[[a]], TRUE, FALSE)
   })
   needList[!names(needList) %in% names(obj)] <- FALSE
   
   implemented <- c("splitSizeDistn", "hasKeys", "ndiv", "nrow", "splitRowDistn")
   needList <- needList[names(needList) %in% implemented]
   
   if(any(needList)) {
      message("*** Running map/reduce job to get missing attributes...")
      map <- rhmap({
         ### rhData
         if(needs["splitSizeDistn"]) rhcollect("splitSizeDistn", object.size(r))
         if(needs["hasKeys"]) rhcollect("hasKeys", k)
         if(needs["ndiv"]) rhcollect("ndiv", 1.0)
         
         ### rhDF
         if(!is.null(trans)) {
            rDF <- trans(r)
            if(needs["nrow"]) rhcollect("nrow", nrow(rDF))
            if(needs["splitRowDistn"]) rhcollect("splitRowDistn", nrow(rDF))
            # deal with summary stuff later...            
         }
      })
      
      reduce <- expression(
         pre={
            ### rhData
            splitSizeDistn <- NULL
            keys <- NULL
            ndiv <- as.numeric(0)
            ### rhDF
            nrow <- as.numeric(0)
            splitRowDistn <- NULL
         },
         reduce={
            ### rhData
            if(reduce.key=="splitSizeDistn")
               splitSizeDistn <- c(splitSizeDistn, do.call(c, reduce.values))
            if(reduce.key=="hasKeys")
               keys <- c(keys, do.call(c, reduce.values))
            if(reduce.key=="ndiv")
               ndiv <- ndiv + sum(unlist(reduce.values), na.rm = TRUE)
            
            ### rhDF
            if(reduce.key=="nrow")
               nrow <- nrow + sum(unlist(reduce.values), na.rm = TRUE)
            if(reduce.key=="splitRowDistn")
               splitRowDistn <- c(splitRowDistn, do.call(c, reduce.values))
         },
         post={
            ### rhData
            if(reduce.key=="splitSizeDistn")
               rhcollect("splitSizeDistn", 
                  quantile(splitSizeDistn, probs=seq(0, 1, by=0.0001), na.rm=TRUE))
            if(reduce.key=="hasKeys")
               rhcollect("hasKeys", keys)
            if(reduce.key=="ndiv")
               rhcollect("ndiv", ndiv)
               
            ### rhDF
            if(reduce.key=="splitRowDistn")
               rhcollect("splitRowDistn", 
                  quantile(splitRowDistn, probs=seq(0, 1, by=0.0001), na.rm=TRUE))
            if(reduce.key=="nrow")
               rhcollect("nrow", nrow)
         }
      )
      
      tmp <- rhwatch(
         map=map, 
         reduce=reduce, 
         input=rhfmt(obj$loc, type=obj$type), 
         mapred=mapred, 
         readback=TRUE, 
         parameters=list(needs = needList, trans=obj$trans)
      )
      
      tmpNames <- sapply(tmp, "[[", 1)

      dataInd <- which(tmpNames %in% rhDataVars)
      dfInd <- which(tmpNames %in% rhDFvars)
      
      # update rhData attributes, if necessary
      if(length(dataInd) > 0) {
         rhDataAttr <- loadRhDataAttr(obj$loc)
         for(i in dataInd) {
            if(tmpNames[i] == "hasKeys") {
               keys <- tmp[[i]][[2]]
               rhsave(keys, file=paste(obj$loc, "/_rh_meta/keys.Rdata", sep=""))
               obj$hasKeys <- TRUE
               rhDataAttr[["hasKeys"]] <- TRUE
            } else {
               rhDataAttr[[tmpNames[i]]] <- tmp[[i]][[2]]
               obj[[tmpNames[i]]] <- tmp[[i]][[2]]
            }
         }
         saveRhDataAttr(rhDataAttr, obj$loc)
      }

      # update rhDF attributes, if necessary
      if(length(dfInd) > 0) {
         rhDFattr <- loadRhDFattr(obj$loc)
         for(i in dfInd) {
            rhDFattr[[tmpNames[i]]] <- tmp[[i]][[2]]
            obj[[tmpNames[i]]] <- tmp[[i]][[2]]
         }
         saveRhDFattr(rhDFattr, obj$loc)
      }
      
      return(obj)
   } else {
      message("All (implemented) attributes have already been computed.")
      return(obj)
   }
}
