#' Load a RHIPE 'rhDF' Object
#'
#' Load / initialize a RHIPE 'rhDF' (RHIPE data.frame) object
#' 
#' @param loc location on HDFS where the data resides (can be a relative path if using \code{\link{hdfs.setwd}})
#' @param type the type of data (map, sequence, text)
#' @param trans a transformation to be applied to the data to coerce it into a data.frame
#' @param reset should existing metadata for this object be overwritten?
#' @param update should a RHIPE job be run to obtain additional attributes for the data prior to returning?
#' @param mapred mapreduce-specific parameters to send to RHIPE job that performs the update (see \code{\link{rhwatch}})
#' 
#' @return an object of class 'rhDF'
#' 
#' @details This function creates an R object representation of collections of key/value pairs data on HDFS where the values are data.frames or easily coerced into data.frames.  The more general case of structureless key/value pair representation in R can be done with \code{\link{rhDdata}}, upon which this class builds.  Several pieces of metadata are stored in an HDFS subdirectory of "loc" called "_rh_meta", including the variables in the data.frame, the total number of rows, the distribution of number of rows, etc. (call \code{print} on one of these objects to see the attributes).  These attributes are useful for subsequent computations that might rely on them.  Some attributes can't be computed in just one pass, in which case \code{\link{updateAttributes}} can be called.
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{rhData}}, \code{\link{updateAttributes}}, \code{\link{getKeys}}, \code{\link{divide.rhData}}
#'
#' @examples
#' \dontrun{
#' 
#' }
#' @export
rhDF <- function(loc, type="sequence", trans=NULL, reset=FALSE, update=FALSE, mapred=NULL) {
   loc <- rhabsolute.hdfs.path(loc)
   
   # first get data attributes
   dataObj <- rhData(loc, type, reset=reset)

   if(is.null(trans))
      trans <- identity
      
   isDF <- inherits(trans(divExample(dataObj)), "data.frame")
   if(!isDF) {
      coerce <- try(as.data.frame(divExample(dataObj)))
      isCoercible <- ifelse(inherits(coerce, "try-error"), FALSE, TRUE)
      # nested lists are coerced quite nicely:
      # as.data.frame(list(a=1:10, b=matrix(nrow=2, ncol=2), c=list(asdf=1, q=7, list(a="asdf")))) # this works
      # as.data.frame(list(var1=1:10, var2=1:3)) # this fails

      if(isCoercible) {
         warning("Data is not strictly a data.frame, but coercible using as.data.frame")
         trans <- as.data.frame
      } else {
         stop("Data in ", loc, " cannot be coerced to be a data.frame")
      }
   }
   
   # if the basic attributes aren't there, make them
   if(!existsOnHDFS(loc, "_rh_meta", "rhDFattr.Rdata") || reset) {
      message("*** Getting basic 'rhDF' attributes...")
      
      dfEx <- trans(divExample(dataObj))
      
      # dfExNames <- names(dfEx)
      # lapply(seq_len(ncol(dfEx)), function(i) {
      #    list(name=dfExNames[i], class=class(dfEx[1,i]))
      # })
      
      # get attributes we don't need to run a MapReduce job for
      # (other attributes can be requested to be filled in later by the user)
      
      rhDFattr <- list(
         vars=lapply(dfEx, class), 
         trans=trans,
         badSchema=NA,
         nrow=NA,
         splitRowDistn=NA,
         summary=NA
      )
      
      saveRhDFattr(rhDFattr, loc)
   } else {
      message("*** Reading in existing 'rhDF' attributes")
      
      # get attributes
      rhDFattr <- loadRhDFattr(loc)

      # set nrow if there is a counter for it
      if(is.na(rhDFattr$nrow)) {
         if(existsOnHDFS(loc, "_rh_meta", "jobData.Rdata")) {
            rhload(paste(loc, "/_rh_meta/jobData.Rdata", sep=""))

            tmp <- jobData$jobInfo$Counters$rhDF         
            if(!is.null(tmp))
               rhDFattr$nrow <- tmp$value
         }
      }
   }
   
   # put it all in one list
   res <- c(rhDFattr, dataObj)
   class(res) <- c("rhDF", class(dataObj))

   if(update)
      res <- updateAttributes(res, mapred=mapred)
      
   res
}
