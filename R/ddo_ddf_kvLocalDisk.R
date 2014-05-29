## Methods for object of class "kvLocalDisk" - key-value pairs as R objects stored on disk

#' @S3method ddoInit localDiskConn
ddoInit.localDiskConn <- function(obj, ...) {
   structure(list(), class = "kvLocalDisk")
}

#' @S3method ddoInitConn localDiskConn
ddoInitConn.localDiskConn <- function(obj, ...) {
   obj
}

#' @S3method requiredObjAttrs kvLocalDisk
requiredObjAttrs.kvLocalDisk <- function(obj) {
   list(
      ddo = c(getDr("requiredDdoAttrs"), "files", "sizes"),
      ddf = getDr("requiredDdfAttrs")
   )
}

#' @S3method getBasicDdoAttrs kvLocalDisk
getBasicDdoAttrs.kvLocalDisk <- function(obj, conn) {
   fp <- conn$loc
   ff <- list.files(fp, recursive = TRUE)
   ff <- ff[!grepl("_meta\\/", ff)]
   if(length(ff) == 0)
      stop("No data")
   sz <- file.info(file.path(fp, ff))$size
   getDat <- function(f) {
      load(f)
      obj
   }
   
   list(
      conn = conn,
      extractableKV = FALSE, 
      totStorageSize = sum(sz),
      files = ff,
      sizes = sz,
      nDiv = length(ff),
      example = getDat(file.path(fp, ff[1]))[[1]]
   )
}

#' @S3method getBasicDdfAttrs kvLocalDisk
getBasicDdfAttrs.kvLocalDisk <- function(obj, transFn) {
   list(
      vars = lapply(kvExample(obj)[[2]], class),
      transFn = transFn
   )
}

# kvLocalDisk is always extractable
#' @S3method hasExtractableKV kvLocalDisk
hasExtractableKV.kvLocalDisk <- function(x) {
   kh <- getAttribute(x, "keyHashes")
   if(length(kh) == 1)
      if(is.na(kh))
         return(FALSE)
   TRUE
}

######################################################################
### extract methods
######################################################################

#' @S3method [ kvLocalDisk
`[.kvLocalDisk` <- function(x, i, ...) {
   # argument i can either be:
   # - a numeric index, in which case the data ff[i] will be obtained
   # - a list of actual keys, in which case the hash function is applied
   #     and matched to the appropriate directory
   # - a hash digest of the desired keys, in which case the appropriate
   #     file will be located

   ff <- getAttribute(x, "files")
   conn <- getAttribute(x, "conn")
   pr <- conn$loc
   nBins <- conn$nBins
   fileHashFn <- conn$fileHashFn
   
   idx <- NULL
   
   if(is.numeric(i)) {
      idx <- i
   } else {
      # try file names, actual keys, and key hash possibilities
      
      # first try file names
      if(any(i %in% ff)) {
         idx <- unlist(lapply(i, function(x) which(ff == x)))
      } else {
         # then try i as actual keys:
         tmp <- try(fileHashFn(i, conn), silent = TRUE)
         if(!inherits(tmp, "try-error"))
            idx <- unlist(lapply(tmp, function(x) which(ff == x)))
         
         if(length(idx) == 0) {
            # now try i as hash, only if it is likely that i is a hash
            if(all(is.character(i))) {
               if(all(nchar(i) == 32)) {
                  if(!hasExtractableKV(x))
                     stop("It appears you are trying to retrive a subset of the data using a hash of the key.  Key hashes have not been computed for this data.  Please call updateAttributes() on this data.")

                  # get the keys that have md5 hashes that match i
                  kh <- getAttribute(x, "keyHashes")
                  tmp <- getKeys(x)[unlist(lapply(i, function(x) which(kh == x)))]
                  tmp <- fileHashFn(tmp, conn)
                  # then get the index of the matching file
                  idx <- unlist(lapply(tmp, function(x) which(ff == x)))
               }
            }
         }
      }
   }
   if(length(idx) == 0)
      return(NULL)
   lapply(idx, function(a) {
      load(file.path(pr, ff[a]))
      obj[[1]]
   })
}

#' @S3method [[ kvLocalDisk
`[[.kvLocalDisk` <- function(x, i, ...) {
   if(length(i) == 1) {
      res <- x[i]
      if(is.null(res)) {
         return(NULL)
      } else {
         return(res[[1]])
      }
   }
}


######################################################################
### convert methods
######################################################################

#' @S3method convertImplemented kvLocalDisk
convertImplemented.kvLocalDisk <- function(obj) {
   c("hdfsConn", "NULL", "localDiskConn")
}

#' @S3method convert kvLocalDisk
convert.kvLocalDisk <- function(from, to = NULL) {
   convertKvLocalDisk(to, from)
}

convertKvLocalDisk <- function(obj, ...)
   UseMethod("convertKvLocalDisk", obj)

# from local disk to local disk
#' @S3method convertKvLocalDisk localDiskConn
convertKvLocalDisk.localDiskConn <- function(to, from, verbose = FALSE) {
   from
}

# from local disk to memory
#' @S3method convertKvLocalDisk NULL
convertKvLocalDisk.NULL <- function(to, from, verbose = FALSE) {
   size <- getAttribute(from, "totObjectSize")
   if(is.na(size))
      size <- getAttribute(from, "totStorageSize")
   if(size / 1024^2 > 100)
      warning("Reading over 100MB of data into memory - probably not a good idea...")
   ff <- getAttribute(from, "files")
   conn <- getAttribute(from, "conn")
   pr <- conn$loc
   res <- do.call(c, lapply(file.path(pr, ff), function(x) {
      load(x)
      obj
   }))
   
   if(inherits(from, "ddf")) {
      res <- ddf(res, update = FALSE, verbose = verbose)
   } else {
      res <- ddo(res, update = FALSE, verbose = verbose)
   }
   
   addNeededAttrs(res, from)
}

# from local disk to HDFS
#' @S3method convertKvLocalDisk hdfsConn
convertKvLocalDisk.hdfsConn <- function(to, from, verbose = FALSE) {
   conn <- getAttribute(from, "conn")
   pr <- conn$loc
   ff <- getAttribute(from, "files")
   
   # TODO: check to make sure "to" is a fresh location
   writeDat <- list()
   objSize <- 0
   for(f in file.path(pr, ff)) {
      load(f)
      writeDat[[length(writeDat) + 1]] <- obj[[1]]
      objSize <- objSize + object.size(obj)
      # flush it to HDFS once the list is bigger than 100MB (make this configurable?)
      if(objSize / 1024^2 > 100) {
         rhwrite(writeDat, file = paste(to$loc, "/", digest(writeDat), "_", object.size(writeDat), sep = ""))
         writeDat <- list()
         objSize <- 0
      }
   }
   if(length(writeDat) > 0)
      rhwrite(writeDat, file = paste(to$loc, "/", digest(writeDat), "_", object.size(writeDat), sep = ""))
   
   if(inherits(from, "ddf")) {
      res <- ddf(to, update = FALSE, verbose = verbose)
   } else {
      res <- ddo(to, update = FALSE, verbose = verbose)
   }
   
   addNeededAttrs(res, from)
}

