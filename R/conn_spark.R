## similar to HDFS connections since

#' Connect to Spark Data Source
#'
#' Connect to a Spark data source, potentially on HDFS
#'
#' @param loc location on the file system (typically HDFS) for the data source
#' @param type is it a "text" file or "object" (default) file?
#' @param hdfs is the file on HDFS?
#' @param init if a SparkContext has not been initialized with \code{sparkR.init}, a named list of arguments to be passed to \code{sparkR.init} to initialize a SparkContext
#' @param autoYes automatically answer "yes" to questions about creating a path if missing
#' @param reset should existing metadata for this object be overwritten?
#' @param verbose logical - print messages about what is being done
#'
#' @return a "kvConnection" object of class "sparkDataConn"
#'
#' @details This simply creates a "connection" to a directory (which need not have data in it).  To actually do things with this data, see \code{\link{ddo}}, etc.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{addData}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{sparkDataConn}}
#'
#' @examples
#' \dontrun{
#'  # connect to empty HDFS directory
#'  conn <- sparkDataConn("/test/irisSplit")
#'  # add some data
#'  addData(conn, list(list("1", iris[1:10,])))
#'  addData(conn, list(list("2", iris[11:110,])))
#'  addData(conn, list(list("3", iris[111:150,])))
#'  # represent it as a distributed data frame
#'  hdd <- ddf(conn)
#' }
#' @export
sparkDataConn <- function(loc, type = "object", hdfs = FALSE, init = list(), autoYes = FALSE, reset = FALSE, verbose = TRUE) {

  if(!requireNamespace("SparkR", quietly = TRUE)) {
    stop("Package 'SparkR' is needed for this function to work. Please install it.",
    call. = FALSE)
  }

  if(!type %in% c("object", "text"))
    stop("'type' must be either 'object' or 'text'")

  if(grepl("^file://", loc)) {
    hdfs <- FALSE
    loc <- gsub("^file://", "", loc)
  }

  if(hdfs) {
    conn <- hdfsConn(loc, autoYes = autoYes, reset = reset, verbose = verbose)
    class(conn)[1] <- "sparkDataConn"
    if(conn$type == "sequence") {
      conn$type <- type
      conn$hdfs <- hdfs
      loc <- conn$loc
      conn$loc <- NULL

      cfg <- .jnew("org/apache/hadoop/conf/Configuration")
      conn$hdfsURI <- .jcall(cfg, "S", "get", "fs.defaultFS")

      metaDir <- paste(loc, "/_meta", sep = "")
      connPath <- paste(metaDir, "/conn.Rdata", sep = "")
      rhsave(conn, file = connPath)
      rhchmod(connPath, "777")

      conn$loc <- loc
    }
  } else {
    conn <- localDiskConn(loc, autoYes = autoYes, reset = reset, verbose = verbose)
    class(conn)[1] <- "sparkDataConn"

    # if it has nBins, etc., then it was just initialized
    if(!is.null(conn$nBins)) {
      # get rid of local disk-specific attributes
      conn$nBins <- NULL
      conn$fileHashFn <- NULL
      conn$type <- type
      conn$hdfs <- hdfs
      loc <- conn$loc
      conn$loc <- NULL
      save(conn, file = file.path(loc, "_meta", "conn.Rdata"))
      conn$loc <- loc
    }
  }

  if(exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
    message("* Retrieving existing SparkContext")
    if(length(init) > 0)
      message("** note: 'init' parameter ignored since SparkContext already exists")
  } else {
    message("* Initializting SparkContext")
    sc <- do.call(sparkR.init, init)
  }

  conn
}

getSparkContext <- function() {
  get(".sparkRjsc", envir = SparkR:::.sparkREnv)
}

#' @export
addData.sparkDataConn <- function(conn, data, overwrite = FALSE) {
  validateListKV(data)

  if(overwrite)
    warning("For SparkR, the ability to overwrite is not supported")

  pdat <- parallelize(getSparkContext(), data)

  ## TODO: need to handle both hdfs and local
  if(conn$hdfs) {
    ff <- rhls(conn$loc)$file
    ff <- ff[grepl("^data[0-9]+$", ff)]
  } else {
    ff <- list.files(conn$loc, pattern = "^data[0-9]+$")
  }

  saveAsObjectFile(pdat, paste(conn$hdfsURI, conn$loc, "/data", length(ff), sep = ""))
}

# takes a list of keys and removes data with those keys
#' @export
removeData.sparkDataConn <- function(conn, keys) {
  message("* Sorry... removing individual k/v pairs from a Spark data connection is currently not supported.")
}

#' @export
print.sparkDataConn <- function(x, ...) {
  cat(paste("sparkDataConn connection (experimental)\n ", ifelse(x$hdfs, "(hdfs)", "(local)"), " loc=", x$loc, "\n", sep = ""))
}

############################################################################
### internal
############################################################################

#' @export
loadAttrs.sparkDataConn <- function(obj, type = "ddo") {
  attrFile <- paste(obj$loc, "/_meta/", type, ".Rdata", sep = "")

  if(obj$hdfs) {
    if(existsOnHDFS(attrFile)) {
      rhload(attrFile)
      if(type == "ddo")
        attrs$conn$loc <- obj$loc
      return(attrs)
    } else {
      return(NULL)
    }
  } else {
    if(file.exists(attrFile)) {
      load(attrFile)
      if(type == "ddo")
        attrs$conn$loc <- obj$loc
      return(attrs)
    } else {
      return(NULL)
    }
  }
}

#' @export
saveAttrs.sparkDataConn <- function(obj, attrs, type = "ddo") {
  fp <- paste(obj$loc, "/_meta/", type, ".Rdata", sep = "")
  if(type == "ddo")
    attrs$conn$loc <- NULL

  if(obj$hdfs) {
    rhsave(attrs, file = fp)
    rhchmod(fp, "777")
  } else {
    save(attrs, file = fp)
  }
}

#' @export
mrCheckOutputLoc.sparkDataConn <- function(x, overwrite = FALSE) {
  if(x$hdfs) {
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
        x <- sparkDataConn(x$loc, type = x$type, hdfs = x$hdfs, autoYes = TRUE, verbose = FALSE)
      }
    }
  } else {
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
        x <- sparkDataConn(x$loc, type = x$type, hdfs = x$hdfs, autoYes = TRUE, verbose = FALSE)
      }
    }
  }
  x
}




# # for now, SparkR data will just come from R objects
# # HDFS support to come...

# #' @export
# loadAttrs.sparkDataConn <- function(obj, type = "ddo") {
#   NULL
# }

# #' @export
# saveAttrs.sparkDataConn <- function(obj, attrs, type = "ddo") {
#   NULL
# }

# #' @export
# print.sparkDataConn <- function(x, ...) {
#   cat("Spark data connection (experimental)")
# }

# #' Connect to Spark Data Source
# #'
# #' Connect to Spark data source (experimental).
# #'
# #' @param data an RDD, a data frame, list of key-value pairs, or location of an RDD on HDFS (to be implemented)
# #' @param init if a SparkContext has not been initialized with \code{sparkR.init}, a named list of arguments to be passed to \code{sparkR.init} to initialize a SparkContext
# #' @param verbose logical - print messages about what is being done
# #'
# #' @note This is currently a proof-of-concept.  It only allows in-memory data to be initialized as a Spark RDD, which is quite pointless for big data.  In the future, this will allow connections to link to data on HDFS.
# #'
# #' @export
# sparkDataConn <- function(data = NULL, sc = NULL, init = list(), verbose = TRUE) {
#   if (!requireNamespace("SparkR", quietly = TRUE)) {
#    stop("Package 'SparkR' is needed for this function to work. Please install it.",
#    call. = FALSE)
#   }

#   if(is.null(data)) {
#    conn <- list(data = NULL, sc = sc)
#   } else {
#    if(!inherits(sc, "jobjRef")) {
#     if(exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
#       message("* Retrieving existing SparkContext")
#       sc <- get(".sparkRjsc", envir = SparkR:::.sparkREnv)
#     } else {
#       message("* Initializting SparkContext")
#       sc <- do.call(sparkR.init, init)
#     }
#    }

#    if(is.null(data) || !inherits(data, c("RDD", "list", "data.frame")))
#     stop("'data' argument must be an RDD, a data frame, list of key-value pairs, or a location of an RDD on HDFS (to be implemented)")

#    if(!inherits(data, "RDD")) {
#     if(inherits(data, "data.frame"))
#       data <- list(list("", data))

#     validateListKV(data)

#     data <- parallelize(sc, data)
#    }

#    conn <- list(data = data, sc = sc)
#   }

#   class(conn) <- c("sparkDataConn", "kvConnection")
#   conn
# }

# mrCheckOutputLoc.sparkDataConn <- function(x, overwrite = FALSE)
#   x
