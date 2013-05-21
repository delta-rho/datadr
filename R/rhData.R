#' Load a RHIPE 'rhData' Object
#'
#' Load / initialize a RHIPE 'rhData' object
#'
#' @param loc location on HDFS where the data resides (can be a relative path if using \code{\link{hdfs.setwd}})
#' @param type the type of data (map, sequence, text)
#' @param reset should existing metadata for this object be overwritten?
#' @param update should a RHIPE job be run to obtain additional attributes for the data prior to returning?
#' @param mapred mapreduce-specific parameters to send to RHIPE job that performs the update (see \code{\link{rhwatch}})
#'
#' @return an object of class 'rhData'
#'
#' @details This function creates an R object representation of any arbitrary set of key/value pair data on HDFS.  See \code{\link{rhDF}} for HDFS data objects that behave like distributed data.frames.  Several pieces of metadata are stored in a subdirectory of "loc" called "_rh_meta", including attributes such as the number of key/value pairs, the distribution of their size, data related to the job that created this data (if any), etc. (call \code{print} on one of these objects to see the attributes).  These attributes are useful for subsequent computations that might rely on them.  Some attributes can't be computed in just one pass, in which case \code{\link{updateAttributes}} can be called.
#'
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{rhDF}}, \code{\link{updateAttributes}}, \code{\link{getKeys}}, \code{\link{divide.rhData}}
#'
#' @examples
#' \dontrun{
#' 
#' }
#' @export
rhData <- function(loc, type="sequence", reset=FALSE, update=FALSE, mapred=NULL) {
   # TODO: try to validate "type" here
   
   # loc <- "pulseDF"
   loc <- rhabsolute.hdfs.path(loc)
   
   if(reset) { # get rid of metadata
      message("*** Resetting (removing old 'rhData' attributes)...")
      if(existsOnHDFS(loc, "_rh_meta", "rhDataAttr.Rdata"))
         rhdel(paste(loc, "/_rh_meta/rhDataAttr.Rdata", sep=""))
      if(existsOnHDFS(loc, "_rh_meta", "keys.Rdata"))
         rhdel(paste(loc, "/_rh_meta/keys.Rdata", sep=""))
      # do rhDF here too
      if(existsOnHDFS(loc, "_rh_meta", "rhDFattr.Rdata"))
         rhdel(paste(loc, "/_rh_meta/rhDFattr.Rdata", sep=""))
      if(existsOnHDFS(loc, "_rh_meta", "rhDivAttr.Rdata"))
         rhdel(paste(loc, "/_rh_meta/rhDivAttr.Rdata", sep=""))
   }
   
   if(existsOnHDFS(loc, "_rh_meta", "rhDFattr.Rdata"))
      message("*** rhDF attributes exist for this data - use rhDF() instead of rhData().")
   
   # if the basic attributes aren't there, make them
   if(!(existsOnHDFS(loc, "_rh_meta", "rhDataAttr.Rdata") && existsOnHDFS(loc, "_rh_meta", "example.Rdata"))) {
      message("*** Getting basic 'rhData' attributes...")
      
      sourceData <- NA
      sourceJobData <- NA
      ndiv <- NA
      if(existsOnHDFS(loc, "_rh_meta", "jobData.Rdata")) {
         rhload(paste(loc, "/_rh_meta/jobData.Rdata", sep=""))
         sourceJobData <- jobData
         class(sourceJobData) <- c("rhJobData", "list")
         tmp <- strsplit(jobData$jobConf$lines$rhipe_input_folder, ",")[[1]]
         # not sure if this will always work...
         sourceData <- unique(dirname(tmp))
         tmp <- jobData$jobInfo$Counters$`Map-Reduce Framework`
         ndiv <- tmp$value[tmp$name=="Reduce output records"]
      }
      
      # number of files on HDFS
      # TODO: be more thorough here: dir.contain.mapfolders, is.mapfolder
      paths <- rhls(loc)
      paths <- paths[!grepl(rhoptions()$file.types.remove.regex, paths$file),,drop=FALSE]
      # if they are mapfiles, the size will be zero
      
      if(any(paths$size == 0)) {
         totSize <- sum(sapply(paths$file, function(a) {
            tmp <- rhls(a)
            remr <- grep(rhoptions()$file.types.remove.regex, tmp)
            if(length(remr) > 0)
               tmp <- tmp[-remr]
            sum(tmp$size)
         }))
      } else {
         totSize <- sum(paths$size)
      }
      
      # get attributes we don't need to run a MapReduce job for
      # (other attributes can be requested to be filled in later by the user)
      rhDataAttr <- list(
         hasKeys=existsOnHDFS(loc, "_rh_meta", "keys.Rdata"),
         loc=loc, 
         type=type,
         nfile=nrow(paths),
         totSize=totSize,
         ndiv=ndiv,
         splitSizeDistn=NA,
         sourceData=sourceData,
         sourceJobData=sourceJobData,
         mapfile=NA
      )
      
      saveRhDataAttr(rhDataAttr, loc)
      
      tmp <- unclass(rhls(loc, rec=TRUE)["file"])$file
      remr <- grep(rhoptions()$file.types.remove.regex, tmp)
      if(length(remr) > 0)
         tmp <- tmp[-remr]
      example <- rhread(tmp, type=type, max=2)
      
      rhsave(example, file=paste(loc, "/_rh_meta/example.Rdata", sep=""))
   } else {
      message("*** Reading in existing 'rhData' attributes")
      # get attributes
      rhDataAttr <- loadRhDataAttr(loc)
      rhload(paste(loc, "/_rh_meta/example.Rdata", sep=""))      
   }
   
   # get mapfile pointer if it is a mapfile
   # we cannot save this attribute - must grab it on each initialization
   if(rhDataAttr$type=="map")
      rhDataAttr$mapfile <- rhmapfile(rhDataAttr$loc)
   
   # check for rhDivAttr.Rdata 
   divClass <- NULL
   if(existsOnHDFS(loc, "_rh_meta", "rhDivAttr.Rdata")) {
      rhDataAttr$divBy <- loadRhDivAttr(loc)
      divClass <- "rhDiv"
   }
   
   # put it all in one list
   res <- c(rhDataAttr, list(example=example))
   class(res) <- c("rhData", divClass, "list")
   
   if(update)
      res <- updateAttributes(res, backendOpts=backendOpts)
      
   res
}


