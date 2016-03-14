#' Experimental sequential text reader helper function
#'
#' Experimental helper function for reading text data sequentially from a file on disk and adding to connection using \code{\link{addData}}
#' @param input the path to an input text file
#' @param output an output connection such as those created with \code{\link{localDiskConn}}, and \code{\link{hdfsConn}}
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param linesPerBlock how many lines at a time to read
#' @param fn function to be applied to each chunk of lines (see details)
#' @param header does the file have a header
#' @param skip number of lines to skip before reading
#' @param recordEndRegex an optional regular expression that finds lines in the text file that indicate the end of a record (for multi-line records)
#' @param cl a "cluster" object to be used for parallel processing, created using \code{makeCluster}
#' @details The function \code{fn} should have one argument, which should expect to receive a vector of strings, each element of which is a line in the file.  It is also possible for \code{fn} to take two arguments, in which case the second argument is the header line from the file (some parsing methods might need to know the header).
#' @examples
#' csvFile <- file.path(tempdir(), "iris.csv")
#' write.csv(iris, file = csvFile, row.names = FALSE, quote = FALSE)
#' myoutput <- localDiskConn(file.path(tempdir(), "irisText"), autoYes = TRUE)
#' a <- readTextFileByChunk(csvFile,
#'   output = myoutput, linesPerBlock = 10,
#'   fn = function(x, header) {
#'     colNames <- strsplit(header, ",")[[1]]
#'     read.csv(textConnection(paste(x, collapse = "\n")), col.names = colNames, header = FALSE)
#'   })
#' a[[1]]
#' @export
readTextFileByChunk <- function(input, output, overwrite = FALSE, linesPerBlock = 10000, fn = NULL, header = TRUE, skip = 0, recordEndRegex = NULL, cl = NULL) {

  if(!is.null(cl) && !is.null(recordEndRegex))
    stop("Currently, cannot read in in parallel when recordEndRegex is specified")

  if(is.null(fn))
    fn <- identity

  con <- file(description = input, open = "r")
  on.exit(close(con))

  curPos <- 1

  if(skip > 0) {
    garbage <- readLines(con, n = skip)
    curPos <- curPos + skip
  }

  if(header) {
    header <- readLines(con, n = 1)
    curPos <- curPos + 1
  }

  data <- readLines(con, n = linesPerBlock)

  i <- 1
  repeat {
    cat("Processing chunk ", i, "\n")
    if (length(data) == 0)
      break

    if(!is.null(recordEndRegex)) {
      cutoff <- max(which(grepl(recordEndRegex, data)))
      extra <- utils::tail(data, length(data) - cutoff)
      data <- utils::head(data, cutoff)
    } else {
      extra <- NULL
    }

    # process current chunk
    formalsLength <- length(formals(fn))
    if(formalsLength == 1) {
      res <- fn(data)
    } else if(formalsLength == 2) {
      res <- fn(data, header)
    } else {
      stop("argument 'fn' must take one or two arguments (if two, second is the header)")
    }

    if(!is.null(res))
      addData(output, list(list(i, res)), overwrite = overwrite)

    data <- tryCatch({
      data <- c(extra, readLines(con, n = linesPerBlock))
    }, error = function(err) {
      if (identical(conditionMessage(err), "no lines available in input"))
        extra
      else stop(err)
    })
    i <- i + 1
  }
  ddo(output)
}

#' Experimental HDFS text reader helper function
#'
#' Experimental helper function for reading text data on HDFS into a HDFS connection
#' @param input a RHIPE input text handle created with \code{rhfmt}
#' @param output an output connection such as those created with \code{\link{localDiskConn}}, and \code{\link{hdfsConn}}
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param fn function to be applied to each chunk of lines (input to function is a vector of strings)
#' @param keyFn optional function to determine the value of the key for each block
#' @param linesPerBlock how many lines at a time to read
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param update should a MapReduce job be run to obtain additional attributes for the result data prior to returning?
#' @examples
#' \dontrun{
#' res <- readHDFStextFile(
#'   input = Rhipe::rhfmt("/path/to/input/text", type = "text"),
#'   output = hdfsConn("/path/to/output"),
#'   fn = function(x) {
#'     read.csv(textConnection(paste(x, collapse = "\n")), header = FALSE)
#'   }
#' )
#' }
#' @export
readHDFStextFile <- function(input, output = NULL, overwrite = FALSE, fn = NULL, keyFn = NULL, linesPerBlock = 10000, control = NULL, update = FALSE) {
  if(!inherits(input, "kvHDFS"))
    stop("Input must be data from a HDFS connection")

  if(getAttribute(input, "conn")$type != "text")
    stop("This was designed to read in HDFS text files")

  if(is.null(fn))
    fn <- identity

  if(is.null(control))
    control <- defaultControl(input)

  if(is.null(control$mapred$rhipe_map_buff_size))
    control$mapred$rhipe_map_buff_size <- linesPerBlock
  if(is.null(control$mapred$mapred.reduce.tasks)) {
    reduce <- 0
  } else {
    reduce <- control$mapred$mapred.reduce.tasks
  }

  map <- expression({
    tmp <- paste(unlist(map.values), collapse = "\n")
    if(is.null(keyFn))
      keyFn <- digest

    collect(keyFn(tmp), fn(tmp))
  })

  # if the user supplies output as an unevaluated connection
  # the verbosity can be misleading
  suppressMessages(output <- output)

  mrExec(input,
    setup    = setup,
    map     = map,
    reduce   = reduce,
    output   = output,
    overwrite = overwrite,
    control  = control,
    params   = list(fn = fn, keyFn = keyFn),
    packages = "digest"
  )
}
