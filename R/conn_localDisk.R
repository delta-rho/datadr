#' Connect to Data Source on Local Disk
#' 
#' Connect to a data source on local disk
#' 
#' @param loc location on local disk for the data source
#' @param nBins number of bins (subdirectories) to put data files into - if anticipating a large number of k/v pairs, it is a good idea to set this to something bigger than 0
#' @param autoYes automatically answer "yes" to questions about creating a path on local disk
#' @param reset should existing metadata for this object be overwritten?
#' @param verbose logical - print messages about what is being done
#' 
#' @return a "kvConnection" object of class "localDiskConn"
#' 
#' @details This simply creates a "connection" to a directory on local disk (which need not have data in it).  To actually do things with this connection, see \code{\link{ddo}}, etc.
#' 
#' @author Ryan Hafen
#' 
#' @seealso \code{addData}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{localDiskConn}}
#' 
#' @examples
#' # connect to empty localDisk directory
#' conn <- localDiskConn(file.path(tempdir(), "irisSplit"), autoYes=TRUE)
#' # add some data
#' addData(conn, list(list("1", iris[1:10,])))
#' addData(conn, list(list("2", iris[11:110,])))
#' addData(conn, list(list("3", iris[111:150,])))
#' # represent it as a distributed data frame
#' irisDdf <- ddf(conn, update=TRUE)
#' irisDdf
#' @export
localDiskConn <- function(loc, nBins=0, autoYes=FALSE, reset=FALSE, verbose=TRUE) {
   if(length(loc) > 1)
      return("A local disk connection must be one directory")
   
   if(!isAbsolutePath(loc)) {
      if(verbose)
         message("* 'loc' is not an absolute path - prepending working directory")
      loc <- file.path(getwd(), loc)
   }
   
   if(!file.exists(loc)) {
      if(autoYes) {
         ans <- "y"
      } else {
         ans <- readline(paste("The path '", loc, "' does not exist.  Should it be created? (y = yes) ", sep=""))
      }
   	if(!tolower(substr(ans, 1, 1)) == "y")
   	   stop("Backing out...")
      
      if(verbose)
         message("* Attempting to create directory ... ", appendLF=FALSE)
      stopifnot(dir.create(loc))
      if(verbose)
         message("ok")
   } else {
      fi <- file.info(loc)
      if(!fi$isdir)
         stop("'loc' must be a directory")
   }
   
   conn <- list(
      loc = normalizePath(loc),
      nBins = nBins
   )
   class(conn) <- c("localDiskConn", "kvConnection")
   
   if(!file.exists(file.path(loc, "_meta"))) {
      if(verbose)
         message("* Saving connection attributes")
      dir.create(file.path(loc, "_meta"))
      save(conn, file=file.path(loc, "_meta", "conn.Rdata"))
   } else if(!reset) {
      if(verbose)
         message("* Loading connection attributes")
      if(file.exists(file.path(loc, "_meta", "conn.Rdata")))
         load(file.path(loc, "_meta", "conn.Rdata"))
         # TODO: message that specified parameters are overridden?
   } else {
      save(conn, file=file.path(loc, "_meta", "conn.Rdata"))
      # if there are "ddo" attributes, we need to update the conn info there
      if(file.exists(file.path(loc, "_meta", "ddo.Rdata"))) {
         load(file.path(loc, "_meta", "ddo.Rdata"))
         attrs$conn <- conn
         attrs$prefix <- loc
         save(attrs, file=file.path(loc, "_meta", "ddo.Rdata"))
      }
   }
   
   if(length(list.files(loc)) <= 1) {
      if(verbose)
         message("* Directory is empty - use addData() to add k/v pairs to this directory")
   } else if(!file.exists(file.path(loc, "_meta", "ddo.Rdata"))) {
      if(verbose)
         message("* To initialize the data in this directory as a distributed data object of data frame, call ddo() or ddf()")
   }
   
   conn
}

#' @S3method addData localDiskConn
addData.localDiskConn <- function(conn, data, overwrite=FALSE) {
   # takes a list of k/v pair lists and writes them to "conn"
   # if a k/v pair with the same key exists, it will overwrite if TRUE
   # for now, assumes unique keys
   
   validateListKV(data)
   
   # if(conn$charKeys)
   #    if(!all(sapply(data, function(x) is.character(x[[1]]))))
   #       stop("This connection expects keys to be characters only")
   
   keys <- lapply(data, "[[", 1)
   filePaths <- getFileLocs(conn, keys)
   
   for(i in seq_along(filePaths)) {
      obj <- data[i]
      if(file.exists(filePaths[i])) {
         if(overwrite) {
            save(obj, file=filePaths[i])
         } else {
            warning(paste("Element", i, "of input data already has data already on disk of the same key.  Set overwrite to TRUE or change key of input data."))
         }
      } else {
         save(obj, file=filePaths[i])
      }
   }
}

# takes a list of keys and removes data with those keys
#' @S3method removeData localDiskConn
removeData.localDiskConn <- function(conn, keys) {
   conn
}

#' @S3method print localDiskConn
print.localDiskConn <- function(x) {
   cat(paste("localDiskConn connection\n  loc=", x$loc, "; nBins=", x$nBins, sep=""))
}

############################################################################
### internal
############################################################################

#' @S3method loadAttrs localDiskConn
loadAttrs.localDiskConn <- function(obj, type="ddo") {
   attrFile <- file.path(obj$loc, "_meta", paste(type, ".Rdata", sep=""))
   if(file.exists(attrFile)) {
      load(attrFile)
      return(attrs)
   } else {
      return(NULL)
   }
}

#' @S3method saveAttrs localDiskConn
saveAttrs.localDiskConn <- function(obj, attrs, type="ddo") {
   attrFile <- file.path(obj$loc, "_meta", paste(type, ".Rdata", sep=""))
   save(attrs, file=attrFile)
}

getFileLocs <- function(conn, keys, digest=TRUE) {
   if(digest) {
      digestKeys <- sapply(keys, digest)      
   } else {
      digestKeys <- keys
   }
   if(conn$nBins == 0) {
      keySubDirs <- rep("", length(digestKeys))
   } else {
      keySubDirs <- sapply(digestKeys, function(x) digestKeyHash(x, conn$nBins))
      padding <- paste("%0", nchar(as.character(conn$nBins)), "d", sep="")
      keySubDirs <- sprintf(padding, keySubDirs)
   }
   fileLocs <- list(digestKeys=digestKeys, keySubDirs=keySubDirs)
   
   if(any(duplicated(fileLocs$digestKeys)))
      stop("There are duplicate keys - not currently supported")
      
   fileDirs <- file.path(conn$loc, fileLocs$keySubDirs)
   uFileDirs <- unique(fileDirs)
   for(fp in uFileDirs)
      if(!file.exists(fp)) dir.create(fp)

   file.path(fileDirs, paste(fileLocs$digestKeys, ".Rdata", sep=""))
}




