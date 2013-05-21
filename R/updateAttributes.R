#' Update Attributes of a 'rhData' or 'rhDF' Object
#'
#' Update attributes of a 'rhData' or 'rhDF' object
#' 
#' @param obj an object of class 'rhData' or 'rhDF'
#' @param backendOpts parameters specifying how the backend should handle things (most-likely parameters to \code{\link{rhwatch}} in RHIPE) - see \code{\link{rhipeControl}}
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
updateAttributes <- function(obj, backendOpts=NULL) {
   if(is.null(backendOpts))
      backendOpts <- defaultControl(obj)
   
   rhDataVars <- c("hasKeys", "ndiv", "splitSizeDistn")
   rhDFvars <- c("nrow", "splitRowDistn", "summary", "badSchema")
   
   updateableVars <- c(rhDataVars, rhDFvars)
   
   needList <- sapply(updateableVars, function(a) {
      ifelse(is.null(obj[[a]]) || is.na(obj[[a]]), TRUE, FALSE)
   })
   needList["hasKeys"] <- !obj[["hasKeys"]]
   
   needList[!names(needList) %in% names(obj)] <- FALSE
   
   implemented <- c("splitSizeDistn", "hasKeys", "ndiv", "nrow", "splitRowDistn", "summary")
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
            r <- trans(r)
         }
         if(needs["nrow"]) rhcollect("nrow", nrow(r))
         if(needs["splitRowDistn"]) rhcollect("splitRowDistn", nrow(r))
         if(needs["summary"]) {
            dfNames <- names(r)
            quantTypes <- c("integer", "numeric", "double")
            categTypes <- c("character", "factor")
            
            for(i in seq_along(r)) {
               var <- r[[i]]
               if(inherits(var, quantTypes)) {
                  rhcollect(paste("summary_quant_", dfNames[i], sep="_"), list(
                     nna=length(which(is.na(r[[i]]))),
                     moments=datadr:::calculateMoments(r[[i]], order=4),
                     min=min(var, na.rm=TRUE),
                     max=max(var, na.rm=TRUE)                     
                  ))
               } else if(inherits(var, categTypes)) {
                  rhcollect(paste("summary_categ_", dfNames[i], sep="_"), list(
                     nna=length(which(is.na(var))),
                     freqTable=datadr:::rhTabulateMap(~ var, data=data.frame(var))
                  ))
               }
            }
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
            ### rhDF summary
            resQuant <- list(nobs=NULL, nna=NULL, moments=NULL, min=NULL, max=NULL)
            resCateg <- list(nobs=NULL, nna=NULL, freqTable=NULL)
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
            
            if(grepl("^summary_quant", reduce.key)) {
               resQuant$nna       <- sum(c(resQuant$nna, sapply(reduce.values, function(x) x$nna)), na.rm=TRUE)
               resQuant$moments   <- do.call(datadr:::combineMultipleMoments, lapply(reduce.values, function(x) x$moments))
               resQuant$min       <- min(c(resQuant$min, sapply(reduce.values, function(x) x$min)), na.rm=TRUE)
               resQuant$max       <- max(c(resQuant$max, sapply(reduce.values, function(x) x$max)), na.rm=TRUE)            
            }
            
            if(grepl("^summary_categ", reduce.key)) {
               resCateg$nna       <- sum(c(resCateg$nna, sapply(reduce.values, function(x) x$nna)), na.rm=TRUE)
               resCateg$freqTable <- datadr:::rhTabulateReduce(resCateg$freqTable, lapply(reduce.values, function(x) x$freqTable))
            }
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
            
            if(grepl("^summary_quant", reduce.key))
               rhcollect(reduce.key, resQuant)
            
            if(grepl("^summary_categ", reduce.key))
               rhcollect(reduce.key, resCateg)
         }
      )
      
      setup <- expression({
         suppressMessages(require(datadr))
      })
      
      tmp <- rhwatch(
         setup=setup,
         map=map, 
         reduce=reduce, 
         input=rhfmt(obj$loc, type=obj$type), 
         mapred=backendOpts$mapred, 
         combiner=backendOpts$combiner, 
         readback=TRUE, 
         parameters=list(needs = needList, trans=obj$trans)
      )
      
      tmpNames <- sapply(tmp, "[[", 1)
      
      dataInd <- which(tmpNames %in% rhDataVars)
      dfInd <- which(tmpNames %in% rhDFvars)
      hasSummary <- any(grepl("^summary", tmpNames))
      
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
      if(length(dfInd) > 0 || hasSummary) {
         rhDFattr <- loadRhDFattr(obj$loc)
         for(i in dfInd) {
            rhDFattr[[tmpNames[i]]] <- tmp[[i]][[2]]
            obj[[tmpNames[i]]] <- tmp[[i]][[2]]
         }
         if(hasSummary) {
            if(is.na(rhDFattr$summary)) {
               summaryList <- list()
            } else {
               summaryList <- rhDFattr$summary
            }
            summaryInd <- which(grepl("^summary", tmpNames))
            for(i in summaryInd) {
               k <- tmp[[i]][[1]]
               v <- tmp[[i]][[2]]
               varType <- gsub("^summary_(quant|categ).*", "\\1", k)
               varName <- gsub("^summary_(quant|categ)__(.*)", "\\2", k)
               if(varType=="quant") {
                  stats <- moments2statistics(v$moments)
                  summaryList[[varName]] <- list(nna=v$nna, stats=stats, range=c(v$min, v$max))
               } else {
                  summaryList[[varName]] <- v
               }
            }
            rhDFattr$summary <- summaryList
            obj[["summary"]] <- summaryList
         }
         saveRhDFattr(rhDFattr, obj$loc)
      }
      
      return(obj)
   } else {
      message("All (implemented) attributes have already been computed.")
      return(obj)
   }
}
