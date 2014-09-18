
# it would be ideal to have a backend-agnostic table reader
# that calls MapReduce, but instead we will do local disk and
# hdfs separately - parallel reading from disk is probably not
# any more efficient then sequential, and requires another pass
# through the data to get byte offsets, and then each parallel
# task has to scan to that offset

#' Data Input
#' 
#' Reads a text file in table format and creates a distributed data frame from it, with cases corresponding to lines and variables to fields in the file.
#' 
#' @param file input text file - can either be character string pointing to a file on local disk, or an \code{\link{hdfsConn}} object pointing to a text file on HDFS (see \code{output} argument below)
#' @param header this and parameters other parameters below are passed to \code{\link{read.table}} for each chunk being processed - see \code{\link{read.table}} for more info.  Most all have defaults or appropriate defaults are set through other format-specific functions such as \code{drRead.csv} and \code{drRead.delim}.
#' @param sep see \code{\link{read.table}} for more info
#' @param quote see \code{\link{read.table}} for more info
#' @param dec see \code{\link{read.table}} for more info
#' @param skip see \code{\link{read.table}} for more info
#' @param fill see \code{\link{read.table}} for more info
#' @param blank.lines.skip see \code{\link{read.table}} for more info
#' @param comment.char see \code{\link{read.table}} for more info
#' @param allowEscapes see \code{\link{read.table}} for more info
#' @param encoding see \code{\link{read.table}} for more info
#' @param \ldots see \code{\link{read.table}} for more info
#' @param autoColClasses should column classes be determined automatically by reading in a sample?  This can sometimes be problematic because of strange ways R handles quotes in \code{read.table}, but keeping the default of \code{TRUE} is advantageous for speed.
#' @param rowsPerBlock how many rows of the input file should make up a block (key-value pair) of output?
#' @param postTransFn a function to be applied after a block is read in to provide any additional processingn before the block is stored
#' @param output a "kvConnection" object indicating where the output data should reside.  Must be a \code{\link{localDiskConn}} object if input is a text file on local disk, or a \code{\link{hdfsConn}} object if input is a text file on HDFS.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of parameters external to the input data that are needed in \code{postTransFn}
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' 
#' @note For local disk, the file is actually read in sequentially instead of in parallel.  This is because of possible performance issues when trying to read from the same disk in parallel.
#' 
#' Note that if \code{skip} is positive and/or if \code{header} is \code{TRUE}, it will first read these in as they only occur once in the data, and we then check for these lines in each block and remove those lines if they appear.
#' 
#' Also note that if you supply \code{"Factor"} column classes, they will be converted to character.
#' 
#' @return an object of class "ddf"
#' 
#' @author Ryan Hafen
#' 
#' @examples
#' \dontrun{   csvFile <- file.path(tempdir(), "iris.csv")
#'    write.csv(iris, file = csvFile, row.names = FALSE, quote = FALSE)
#'    irisTextConn <- localDiskConn(file.path(tempdir(), "irisText2"), autoYes = TRUE)
#'    a <- drRead.csv(csvFile, output = irisTextConn, rowsPerBlock = 10)
#' }
#' @rdname drreadtable
#' @export
drRead.table <- function(file, 
   header = FALSE, 
   sep = "", 
   quote = "\"'", 
   dec = ".", 
   skip = 0,
   fill = !blank.lines.skip, 
   blank.lines.skip = TRUE, 
   comment.char = "#",
   allowEscapes = FALSE,
   encoding = "unknown",
   autoColClasses = TRUE,
   rowsPerBlock = 50000,
   postTransFn = identity,
   output = NULL,
   overwrite = FALSE,
   params = NULL,
   packages = NULL,
   control = NULL,
   ...
   ) {
   
   if(!(is.character(file) || inherits(file, "hdfsConn")))
      stop("file must be a path to a file on disk or an hdfsConn object")
   
   if(is.character(file)) {
      if(!inherits(output, "localDiskConn"))
         stop("output must be a localDiskConn object with input text file on disk")
      file <- normalizePath(file)
      if(file.info(file)$isdir)
         file <- list.files(file, recursive = TRUE, full.names = TRUE)
   } else {
      if(!inherits(output, "hdfsConn"))
         stop("output must be a hdfsConn object with input text file on HDFS")
   }
   
   hdText <- ""
   if(header)
      hdText <- getTextFileHeaderLines(file, skip = skip)
   
   hd <- scan(textConnection(hdText), what = "", sep = sep, quote = quote, nlines = 1, quiet = TRUE, skip = 0, strip.white = TRUE, blank.lines.skip = blank.lines.skip, comment.char = comment.char, allowEscapes = allowEscapes, encoding = encoding)
   
   readTabParams <- list(
      header = FALSE, 
      sep = sep, 
      quote = quote, 
      dec = dec, 
      skip = 0,
      fill = fill, 
      blank.lines.skip = blank.lines.skip, 
      comment.char = comment.char,
      allowEscapes = allowEscapes,
      encoding = encoding,
      ...
   )
   
   message("* testing read on a subset... ", appendLF = FALSE)
   topLines <- getTextFileTopLines(file, skip = skip, header = header)
   
   readTabParamsTmp <- readTabParams
   readTabParamsTmp$header <- FALSE
   readTabParamsTmp$skip <- 0
   readTabParamsTmp$file <- textConnection(paste(topLines, collapse = "\n"))
   res <- do.call(read.table, readTabParamsTmp)
   if(is.null(readTabParamsTmp$col.names))
      names(res) <- hd
   postTransFn(res)
   message("ok")
   
   # to ensure each subset has the same column classes, 
   # set them (if not already set) based on tmp read-in
   if(is.null(readTabParams$colClasses))
      readTabParams$colClasses <- sapply(res, class)
   if(!autoColClasses)
      readTabParams$colClasses <- NULL
   
   # for now, force factors to be character
   readTabParams$colClasses[readTabParams$colClasses == "factor"] <- "character"
   
   # if the user supplies output as an unevaluated connection
   # the verbosity can be misleading
   suppressMessages(output <- output)
   
   readTable(file, rowsPerBlock, skip, header, hd, hdText, readTabParams, postTransFn, output, overwrite, params, packages, control)
}

############################################################################
### method to get headers
############################################################################

getTextFileHeaderLines <- function(x, ...)
   UseMethod("getTextFileHeaderLines", x)

#' @export
getTextFileHeaderLines.character <- function(x, skip) {
   readLines(x[1], n = skip + 1)
}

#' @export
getTextFileHeaderLines.hdfsConn <- function(x, skip) {
   rhread(x$loc, type = "text", max = skip + 1)
}

############################################################################
### method to get top lines
############################################################################

getTextFileTopLines <- function(x, ...)
   UseMethod("getTextFileTopLines", x)

#' @export
getTextFileTopLines.character <- function(x, skip, header, n = 1000) {
   tmp <- readLines(x[1], n = skip + header + n)
   tail(tmp, length(tmp) - skip - header)
}

#' @export
getTextFileTopLines.hdfsConn <- function(x, skip, header, n = 1000) {
   tmp <- rhread(x$loc, type = "text", max = skip + header + n)
   tail(tmp, length(tmp) - skip - header)
}

############################################################################
### method to read in and create ddf
############################################################################

readTable <- function(file, ...)
   UseMethod("readTable", file)

#' @export
readTable.character <- function(file, rowsPerBlock, skip, header, hd, hdText, readTabParams, postTransFn, output, overwrite, params, packages, control) {
   
   i <- 1
   for(ff in file) {
      cat("-- In file ", ff, "\n")
      con <- file(description = ff, open = "r")
      on.exit(close(con))
      curPos <- 1
      
      if(skip > 0) {
         garbage <- readLines(con, n = skip)
         curPos <- curPos + skip
      }
      
      if(header) {
         tmp <- readLines(con, n = 1)
         curPos <- curPos + 1
      }
      
      readTabParams$file <- con
      readTabParams$nrows <- rowsPerBlock
      data <- do.call(read.table, readTabParams)
      
      repeat {
         cat("   Processing chunk ", i, "\n")
         if(is.data.frame(data)) {
            if(nrow(data) == 0) {
               cat("   * End of file - no data for chunk ", i, "\n")
               break
            }
         } else if(is.null(data)) {
            cat("   * End of file - no data for chunk ", i, "\n")
            break
         }
         
         if(is.null(readTabParams$col.names))
            names(data) <- hd
         
         addData(output, list(list(i, postTransFn(data))), overwrite = overwrite)
         
         data <- tryCatch({
            do.call(read.table, readTabParams)
         }, error=function(err) {
            if (identical(conditionMessage(err), "no lines available in input"))
               NULL
            else stop(err)
         })
         i <- i + 1
      }
   }
   
   ddf(output)
}

#' @export
readTable.hdfsConn <- function(file, rowsPerBlock, skip, header, hd, hdText, readTabParams, postTransFn, output, overwrite, params, packages, control) {
   
   map <- expression({
      tmp <- unlist(map.values)
      readTabParams$file <- textConnection(paste(tmp[!tmp %in% hdText], collapse = "\n"))
      res <- do.call(read.table, readTabParams)
      if(is.null(readTabParams$col.names))
         names(res) <- hd
      id <- Sys.getenv("mapred.task.id")
      collect(id, postTransFn(res))
   })
   control$mapred$rhipe_map_buff_size <- format(rowsPerBlock, scientific = FALSE)   
   
   parList <- list(
      skip = skip,
      header = header,
      hd = hd,
      hdText = hdText,
      readTabParams = readTabParams,
      postTransFn = postTransFn
   )
   
   ddf(mrExec(ddo(file), map = map, reduce = 0, control = control, output = output, overwrite = overwrite, params = c(params, parList), packages = packages))
}

#############################################################################
### provide read.csv, read.delim, etc.
############################################################################

#' @rdname drreadtable
#' @export
drRead.csv <- function(file, header = TRUE, sep = ",", quote = "\"", dec = ".", fill = TRUE, comment.char = "", ...) 
   drRead.table(file = file, header = header, sep = sep, quote = quote, dec = dec, fill = fill, comment.char = comment.char, ...)

#' @rdname drreadtable
#' @export
drRead.csv2 <- function(file, header = TRUE, sep = ";", quote = "\"", dec = ",", fill = TRUE, comment.char = "", ...)
   drRead.table(file = file, header = header, sep = sep, quote = quote, dec = dec, fill = fill, comment.char = comment.char, ...)

#' @rdname drreadtable
#' @export
drRead.delim <- function(file, header = TRUE, sep = "\t", quote = "\"", dec = ".", fill = TRUE, comment.char = "", ...) 
   drRead.table(file = file, header = header, sep = sep, quote = quote, dec = dec, fill = fill, comment.char = comment.char, ...)

#' @rdname drreadtable
#' @export
drRead.delim2 <- function(file, header = TRUE, sep = "\t", quote = "\"", dec = ",", fill = TRUE, comment.char = "", ...) 
   drRead.table(file = file, header = header, sep = sep, quote = quote, dec = dec, fill = fill, comment.char = comment.char, ...)

# #' @rdname drreadtable
# #' @export
# drRead.fwf <- function(file, widths, header = FALSE, sep = "\t", skip = 0, row.names, col.names, n = -1, buffersize = 2000, ...)
#    read.fwf(file, widths = width, header = header, sep = sep, skip = sep, row.names = row.names, col.names = col.names, n = n, buffersize = buffersize, ...)

