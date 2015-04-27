#' Connect to Data Source on Local Disk
#'
#' Connect to a data source on local disk
#'
#' @param loc location on local disk for the data source
#' @param nBins number of bins (subdirectories) to put data files into - if anticipating a large number of k/v pairs, it is a good idea to set this to something bigger than 0
#' @param fileHashFn an optional function that operates on each key-value pair to determine the subdirectory structure for where the data should be stored for that subset, or can be specified "asis" when keys are scalar strings
#' @param autoYes automatically answer "yes" to questions about creating a path on local disk
#' @param reset should existing metadata for this object be overwritten?
#' @param verbose logical - print messages about what is being done
#'
#' @return a "kvConnection" object of class "localDiskConn"
#'
#' @details This simply creates a "connection" to a directory on local disk (which need not have data in it).  To actually do things with this connection, see \code{\link{ddo}}, etc.  Typically, you should just use \code{loc} to specify where the data is or where you would like data for this connection to be stored.  Metadata for the object is also stored in this directory.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{addData}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{localDiskConn}}
#'
#' @examples
#' # connect to empty localDisk directory
#' conn <- localDiskConn(file.path(tempdir(), "irisSplit"), autoYes = TRUE)
#' # add some data
#' addData(conn, list(list("1", iris[1:10,])))
#' addData(conn, list(list("2", iris[11:110,])))
#' addData(conn, list(list("3", iris[111:150,])))
#' # represent it as a distributed data frame
#' irisDdf <- ddf(conn, update = TRUE)
#' irisDdf
#' @export
localDiskConn <- function(loc, nBins = 0, fileHashFn = NULL, autoYes = FALSE, reset = FALSE, verbose = TRUE) {
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
      ans <- readline(paste("The path '", loc, "' does not exist.  Should it be created? (y = yes) ", sep = ""))
    }
    if(!tolower(substr(ans, 1, 1)) == "y")
      stop("Backing out...")

    if(verbose)
      message("* Attempting to create directory ... ", appendLF = FALSE)
    stopifnot(dir.create(loc))
    if(verbose)
      message("ok")
  } else {
    fi <- file.info(loc)
    if(!fi$isdir)
      stop("'loc' must be a directory")
  }

  if(is.character(fileHashFn)) {
    if(fileHashFn == "asis") {
      fileHashFn <- charFileHash
    }
  }

  if(is.null(fileHashFn)) {
   fileHashFn <- digestFileHash
  }

  conn <- list(
    nBins = nBins,
    fileHashFn = fileHashFn
  )
  class(conn) <- c("localDiskConn", "kvConnection")

  if(!file.exists(file.path(loc, "_meta"))) {
    ## this must be a location where a connection hasn't existed
    if(verbose)
      message("* Saving connection attributes")
    dir.create(file.path(loc, "_meta"))
    save(conn, file = file.path(loc, "_meta", "conn.Rdata"))
  } else if(!reset) {
    ## load existing connection attributes
    if(verbose)
      message("* Loading connection attributes")
    if(file.exists(file.path(loc, "_meta", "conn.Rdata")))
      load(file.path(loc, "_meta", "conn.Rdata"))
      # TODO: message that specified parameters are overridden by what was read in?
  } else {
    save(conn, file = file.path(loc, "_meta", "conn.Rdata"))
    # if there are "ddo" attributes, we need to update the conn info there
    if(file.exists(file.path(loc, "_meta", "ddo.Rdata"))) {
      load(file.path(loc, "_meta", "ddo.Rdata"))
      attrs$conn <- conn
      save(attrs, file = file.path(loc, "_meta", "ddo.Rdata"))
    }
  }

  if(length(list.files(loc)) <= 1) {
   if(verbose)
    message("* Directory is empty - use addData() to add k/v pairs to this directory")
  } else if(!file.exists(file.path(loc, "_meta", "ddo.Rdata"))) {
   if(verbose)
    message("* To initialize the data in this directory as a distributed data object of data frame, call ddo() or ddf()")
  }

  ## don't store location with connection - user must always specify
  ## location so just use it - this makes it easier for the user
  ## to move data around without a hard-coded path saved in the metadata
  conn$loc <- normalizePath(loc)
  conn
}

#' Character File Hash Function
#'
#' Function to be used to specify the file where key-value pairs get stored for local disk connections, useful when keys are scalar strings.  Should be passed as the argument \code{fileHashFn} to \code{\link{localDiskConn}}.
#'
#' @param keys keys to be hashed
#' @param conn a "localDiskConn" object
#'
#' @details You shouldn't need to call this directly other than to experiment with what the output looks like or to get ideas on how to write your own custom hash.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{localDiskConn}, \code{\link{digestFileHash}}
#'
#' @examples
#' # connect to empty localDisk directory
#' path <- file.path(tempdir(), "irisSplit")
#' unlink(path, recursive = TRUE)
#' conn <- localDiskConn(path, autoYes = TRUE, fileHashFn = charFileHash)
#' # add some data
#' addData(conn, list(list("key1", iris[1:10,])))
#' addData(conn, list(list("key2", iris[11:110,])))
#' addData(conn, list(list("key3", iris[111:150,])))
#' # see that files were stored by their key
#' list.files(path)
#' @export
charFileHash <- function(keys, conn) {
  if(!all(sapply(keys, is.character)) || !all(sapply(keys, length) == 1)) {
   warning("not all keys are scalar strings, but using charFileHash... converting keys to md5")
   files <- sapply(keys, digest)
  } else {
   files <- unlist(keys)
  }
  paste(files, ".Rdata", sep = "")
}


#' Digest File Hash Function
#'
#' Function to be used to specify the file where key-value pairs get stored for local disk connections, useful when keys are arbitrary objects.  File names are determined using a md5 hash of the object.  This is the default argument for \code{fileHashFn} in \code{\link{localDiskConn}}.
#'
#' @param keys keys to be hashed
#' @param conn a "localDiskConn" object
#'
#' @details You shouldn't need to call this directly other than to experiment with what the output looks like or to get ideas on how to write your own custom hash.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{localDiskConn}, \code{\link{charFileHash}}
#'
#' @examples
#' # connect to empty localDisk directory
#' path <- file.path(tempdir(), "irisSplit")
#' unlink(path, recursive = TRUE)
#' conn <- localDiskConn(path, autoYes = TRUE, fileHashFn = digestFileHash)
#' # add some data
#' addData(conn, list(list("key1", iris[1:10,])))
#' addData(conn, list(list("key2", iris[11:110,])))
#' addData(conn, list(list("key3", iris[111:150,])))
#' # see that files were stored by their key
#' list.files(path)
#' @export
digestFileHash <- function(keys, conn) {
  digestKeys <- sapply(keys, digest)

  if(conn$nBins == 0) {
    keySubDirs <- rep("", length(digestKeys))
    res <- paste(digestKeys, ".Rdata", sep = "")
  } else {
    keySubDirs <- sapply(digestKeys, function(x) digestKeyHash(x, conn$nBins))
    padding <- paste("%0", nchar(as.character(conn$nBins)), "d", sep = "")
    keySubDirs <- sprintf(padding, keySubDirs)
    res <- file.path(keySubDirs, paste(digestKeys, ".Rdata", sep = ""))
  }
  res
}

#' @export
addData.localDiskConn <- function(conn, data, overwrite = FALSE) {
  # takes a list of k/v pair lists and writes them to "conn"
  # if a k/v pair with the same key exists, it will overwrite if TRUE
  # for now, assumes unique keys

  validateListKV(data)

  # if(conn$charKeys)
  #  if(!all(sapply(data, function(x) is.character(x[[1]]))))
  #    stop("This connection expects keys to be characters only")

  keys <- lapply(data, "[[", 1)
  filePaths <- getFileLocs(conn, keys)

  for(i in seq_along(filePaths)) {
    obj <- data[i]
    if(file.exists(filePaths[i])) {
      if(overwrite) {
        save(obj, file = filePaths[i])
      } else {
        warning(paste("Element", i, "of input data has data already on disk of the same key.  Set overwrite to TRUE or change key of input data."))
      }
    } else {
      save(obj, file = filePaths[i])
    }
  }
}

# takes a list of keys and removes data with those keys
#' @export
removeData.localDiskConn <- function(conn, keys) {
  conn
}

#' @export
print.localDiskConn <- function(x, ...) {
  cat(paste("localDiskConn connection\n  loc=", x$loc, "; nBins=", x$nBins, sep = ""))
}

#' @export
loadAttrs.localDiskConn <- function(obj, type = "ddo") {
  attrFile <- file.path(obj$loc, "_meta", paste(type, ".Rdata", sep = ""))
  if(file.exists(attrFile)) {
    load(attrFile)
    if(type == "ddo")
      attrs$conn$loc <- obj$loc
    return(attrs)
  } else {
    return(NULL)
  }
}

#' @export
saveAttrs.localDiskConn <- function(obj, attrs, type = "ddo") {
  if(type == "ddo")
    attrs$conn$loc <- NULL
  attrFile <- file.path(obj$loc, "_meta", paste(type, ".Rdata", sep = ""))
  save(attrs, file = attrFile)
}

############################################################################
### internal
############################################################################

getFileLocs <- function(conn, keys) {
  fileLocs <- do.call(conn$fileHashFn, list(keys = keys, conn = conn))
  fileLocs <- file.path(conn$loc, fileLocs)

  if(any(duplicated(fileLocs)))
    stop("There are duplicate keys - not currently supported")

  fileDirs <- dirname(fileLocs)
  uFileDirs <- unique(fileDirs)
  for(fp in uFileDirs)
    if(!file.exists(fp)) dir.create(fp, recursive = TRUE)

  fileLocs
}

#' @export
mrCheckOutputLoc.localDiskConn <- function(x, overwrite = FALSE) {
  if(file.exists(x$loc)) {
    tmp <- list.files(x$loc)
    tmp <- setdiff(tmp, "_meta")
    if(length(tmp) > 0) {
      message(paste("The output path '", x$loc, "' exists and contains data... ", sep = ""), appendLF = FALSE)
      if(overwrite == "TRUE") {
        message("removing existing data...")
        unlink(x$loc, recursive = TRUE)
      } else if(overwrite == "backup") {
        bakFile <- paste(x$loc, "_bak", sep = "")
        message(paste("backing up to ", bakFile, "...", sep = ""))
        if(file.exists(bakFile))
         unlink(bakFile, recursive = TRUE)
        file.rename(x$loc, bakFile)
      } else {
        stop("backing out...", call. = FALSE)
      }
      x <- localDiskConn(x$loc, nBins = x$nBins, fileHashFn = x$fileHashFn, autoYes = TRUE, verbose = FALSE)
    }
  }
  x
}


