


#' Experimental sequential text reader helper function
#' Experimental helper function for reading text data in sequentially and adding to connection using \code{\link{addData}}
#' @export
processTextFileByChunk <- function(inFile, outConn, chunkSize, fn, header=TRUE, skip=0, recordEndRegex = NULL) {
   # inFile <- "data/raw/bb-week2.csv"; chunkSize <- 1000; skip <- 0; header <- TRUE; recordEndRegex <- "\"$"
   
   con <- file(description = inFile, open = "r")
   on.exit(close(con))
   
   if(skip > 0)
      garbage <- readLines(con, n = skip)
   
   if(header)
      header <- readLines(con, n = 1)
   
   data <- readLines(con, n = chunkSize)
   
   i <- 1
   repeat {
      cat("Processing chunk ", i, "\n")
      if (length(data) == 0)
         break
      
      if(!is.null(recordEndRegex)) {
         cutoff <- max(which(grepl(recordEndRegex, data)))
         extra <- tail(data, length(data) - cutoff)
         data <- head(data, cutoff)
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
         addData(outConn, list(list(i, res)))
      
      data <- tryCatch({
         data <- c(extra, readLines(con, n = chunkSize))
      }, error=function(err) {
         if (identical(conditionMessage(err), "no lines available in input"))
            extra
         else stop(err)
      })
      i <- i + 1
   }
}

#' Experimental HDFS text reader helper function
#' Experimental helper function for reading text data into HDFS
#' @export
rhText2df <- function(x, output=NULL, transFn=NULL, keyFn=NULL, linesPerBlock=10000, control=NULL, update=FALSE) {
   if(!inherits(x, "kvHDFS"))
      stop("Input must be data from a HDFS connection")
   
   if(getAttribute(x, "conn")$type != "text")
      stop("This was designed for 'rhData' inputs of type 'text'.")
   
   if(is.null(transFn))
      stop("Must specify a transformation function with 'transFn'.")
      
   if(is.null(control))
      control <- defaultControl(x)
      
   if(is.null(control$mapred$rhipe_map_buff_size))
      control$mapred$rhipe_map_buff_size <- linesPerBlock
   if(is.null(control$mapred$mapred.reduce.tasks)) {
      reduce <- 0
   } else {
      reduce <- control$mapred$mapred.reduce.tasks
   }
   
   setup <- expression({
      suppressWarnings(suppressMessages(require(digest)))
   })
   
   map <- expression({
      tmp <- paste(unlist(map.values), collapse="\n")
      if(is.null(keyFn))
         keyFn <- digest
      
      collect(keyFn(tmp), transFn(tmp))
   })
   
   mrExec(x,
      setup   = setup,
      map     = map, 
      reduce  = reduce, 
      output  = output,
      control = control,
      params  = list(transFn = transFn, keyFn = keyFn)
   )
}
