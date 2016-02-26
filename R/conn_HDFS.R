#' Connect to Data Source on HDFS
#'
#' Connect to a data source on HDFS
#'
#' @param loc location on HDFS for the data source
#' @param type the type of data ("map", "sequence", "text")
#' @param autoYes automatically answer "yes" to questions about creating a path on HDFS
#' @param reset should existing metadata for this object be overwritten?
#' @param verbose logical - print messages about what is being done
#'
#' @return a "kvConnection" object of class "hdfsConn"
#'
#' @details This simply creates a "connection" to a directory on HDFS (which need not have data in it).  To actually do things with this data, see \code{\link{ddo}}, etc.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{addData}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{localDiskConn}}
#'
#' @examples
#' \dontrun{
#'   # connect to empty HDFS directory
#'   conn <- hdfsConn("/test/irisSplit")
#'   # add some data
#'   addData(conn, list(list("1", iris[1:10,])))
#'   addData(conn, list(list("2", iris[11:110,])))
#'   addData(conn, list(list("3", iris[111:150,])))
#'   # represent it as a distributed data frame
#'   hdd <- ddf(conn)
#' }
#' @export
hdfsConn <- function(loc, type = "sequence", autoYes = FALSE, reset = FALSE, verbose = TRUE) {

  loadRhipe()

  if(length(loc) > 1)
    stop("A HDFS connection must be one directory")

  if(!grepl("^\\/", loc)) {
    if(verbose)
      message("* 'loc' is not an absolute path - prepending HDFS working directory")
    loc <- rhabsolute.hdfs.path(loc)
  }

  if(!existsOnHDFS(loc)) {
    if(autoYes) {
      ans <- "y"
    } else {
      ans <- readline(paste("The path '", loc, "' does not exist on HDFS.  Should it be created? (y = yes) ", sep = ""))
    }
  	if(!tolower(substr(ans, 1, 1)) == "y")
  	  stop("Backing out...")

    if(verbose)
      message("* Attempting to create directory... ", appendLF = FALSE)
    if(!rhmkdir(loc)) {
      message("")
      stop()
    }
    if(verbose)
      message("success")
  } else {
    # TODO: make sure it has the right type
  }

  conn <- list(
    type = type
  )

  metaDir <- paste(loc, "/_meta", sep = "")
  connPath <- paste(metaDir, "/conn.Rdata", sep = "")

  nullFile <- ifelse(.Platform$OS.type == "unix", "/dev/null", "NUL")

  if(!existsOnHDFS(metaDir)) {
    if(verbose)
      message("* Saving connection attributes")
    rhmkdir(metaDir)
    rhchmod(metaDir, "777")
    capture.output(rhsave(conn, file = paste(metaDir, "/conn.Rdata", sep = "")), file = nullFile)
    rhchmod(paste(metaDir, "/conn.Rdata", sep = ""), "777")
  } else if(!reset) {
    if(verbose)
      message("* Loading connection attributes")
    if(existsOnHDFS(connPath))
      capture.output(rhload(connPath), file = nullFile)
      # TODO: message that specified parameters are overridden?
  } else {
    rhsave(conn, file = connPath)
    rhchmod(connPath, "777")
  }

  # TODO: better way to check if empty
  if(nrow(rhls(loc)) <= 1) {
    if(verbose)
      message("* Directory is empty... move some data in here")
  } else if(!existsOnHDFS(paste(metaDir, "/ddo.Rdata", sep = ""))) {
    if(verbose)
      message("* To initialize the data in this directory as a distributed data object or data frame, call ddo() or ddf()")
  }

  ## don't store location with connection - user must always specify
  ## location so just use it - this makes it easier for the user
  ## to move data around without a hard-coded path saved in the metadata
  conn$loc <- loc

  class(conn) <- c("hdfsConn", "kvConnection")
  conn
}

#' @export
addData.hdfsConn <- function(conn, data, overwrite = FALSE) {
  # warning("This makes your mapfile go away")
  validateListKV(data)

  # if(conn$charKeys)
  #   if(!all(sapply(data, function(x) is.character(x[[1]]))))
  #     stop("This connection expects keys to be characters only")

  # for now, just save the data with the name being the current system time
  # plus the hash for the data object - this should avoid any collisions
  # if it already exists, append stuff to it

  op <- options(digits.secs = 6)
  filename <- paste(conn$loc, "/", digest(data), "_", as.character(unclass(Sys.time())), sep = "")
  options(op)

  rhwrite(data, filename)
}

# takes a list of keys and removes data with those keys
#' @export
removeData.hdfsConn <- function(conn, keys) {
  message("* Sorry... removing individual k/v pairs from HDFS is not fun. (requires complete read and write of the data).")
}

#' @export
print.hdfsConn <- function(x, ...) {
  cat(paste("hdfsConn connection\n  loc=", x$loc, "; type=", x$type, "\n", sep = ""))
}

############################################################################
### internal
############################################################################

#' @export
loadAttrs.hdfsConn <- function(obj, type = "ddo") {
  attrFile <- paste(obj$loc, "/_meta/", type, ".Rdata", sep = "")

  if(existsOnHDFS(attrFile)) {
    rhload(attrFile)
    if(type == "ddo")
      attrs$conn$loc <- obj$loc
    return(attrs)
  } else {
    return(NULL)
  }
}

#' @export
saveAttrs.hdfsConn <- function(obj, attrs, type = "ddo") {
  fp <- paste(obj$loc, "/_meta/", type, ".Rdata", sep = "")
  if(type == "ddo")
    attrs$conn$loc <- NULL
  rhsave(attrs, file = fp)
  rhchmod(fp, "777")
}

loadRhipe <- function() {
  if(!requireNamespace("Rhipe", quietly = TRUE)) {
    stop("Package 'Rhipe' is needed for this function to work. Please install it.",
    call. = FALSE)
  }

  if(is.null(rhoptions()$server)) {
    message("* RHIPE is not initialized... initializing")
    rhinit()
  }

  # make sure _meta files are ignored
  rgx <- rhoptions()$file.types.remove.regex
  if(!grepl("/_meta", rgx)) {
    rgx <- gsub("^\\(", "(_meta|", rgx)
    rhoptions(file.types.remove.regex = rgx)
  }
}

# check to see if file exists on HDFS
existsOnHDFS <- function(...) {
  params <- list(...)
  path <- rhabsolute.hdfs.path(paste(params, collapse = "/"))
  res <- try(rhls(path), silent = TRUE)
  if(inherits(res, "try-error")) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#' @export
mrCheckOutputLoc.hdfsConn <- function(x, overwrite = FALSE) {
  if(existsOnHDFS(x$loc)) {
    ff <- rhls(x$loc)$file
    ff <- ff[!grepl(rhoptions()$file.types.remove.regex, ff)]
    if(length(ff) > 0) {
      message(paste("The output path '", x$loc, "' exists and contains data... ", sep = ""), appendLF = FALSE)
      if(overwrite == "TRUE") {
        message("removing existing data...")
        rhdel(x$loc)
      } else if(overwrite == "backup") {
        bakFile <- paste(x$loc, "_bak", sep = "")
        message(paste("backing up to ", bakFile, "...", sep = ""))
        if(file.exists(bakFile))
          rhdel(bakFile, recursive = TRUE)
        rhmv(x$loc, bakFile)
      } else {
    	  stop("backing out...", call. = FALSE)
      }

      x <- hdfsConn(x$loc, type = x$type, autoYes = TRUE, verbose = FALSE)
    }
  }
  x
}

