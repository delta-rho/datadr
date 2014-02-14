### mrExec for kvHDFS objects

#' @S3method mrExecInternal kvHDFSList
mrExecInternal.kvHDFSList <- function(data, setup=NULL, map=NULL, reduce=NULL, output=NULL, control=NULL, params=NULL) {
   
   setup2 <- expression({
      collect <- rhcollect
      counter <- rhcounter
      .libPaths(libPaths)
   })
   setup <- appendExpression(setup2, setup)
   
   params$libPaths <- .libPaths()
   
   if(inherits(output, "hdfsConn")) {
      if(is.expression(reduce)) 
         output$type <- "map" # force type="map" if there is a reduce - why not?
   } else if(is.character(output)) {
      output <- hdfsConn(output)
   } else {
      output <- hdfsConn(Rhipe:::mkdHDFSTempFolder(file="tmp_output"), autoYes=TRUE)
   }
   outFile <- rhfmt(output$loc, type=output$type)
   
   # set write.job.info to TRUE
   wji <- rhoptions()$write.job.info
   rhoptions(write.job.info=TRUE)
      
   conns <- lapply(data, function(x) getAttribute(x, "conn"))
   locs <- sapply(conns, function(x) x$loc)
   types <- sapply(conns, function(x) x$type)
   if(length(unique(types)) != 1)
      stop("Currently all inputs must have the same type ('map', 'seq', 'text')")
   
   # create a lookup for file being processed and it's dataSourceName
   nms <- names(data)
   dataSourceName <- lapply(seq_along(data), function(i) nms[i])
   names(dataSourceName) <- locs
   params$dataSourceName <- dataSourceName
   map2 <- expression({
      mif <- Sys.getenv("mapred.input.file")
      .ind <- which(sapply(names(dataSourceName), function(x) grepl(x, mif)))
      .dataSourceName <- dataSourceName[[.ind]]
   })
   map <- appendExpression(map2, map)
   
   co <- rhoptions()$copyObjects
   co2 <- co
   co2$auto <- FALSE
   rhoptions(copyObjects = co2)
   
   res <- rhwatch(
      setup=setup,
      map=map, 
      reduce=reduce, 
      input=rhfmt(locs, type=types[1]),
      output=outFile,
      mapred=control$mapred, 
      # combiner=control$combiner,
      readback=FALSE, 
      parameters=params
   )
   
   rhoptions(copyObjects = co)
   # set write.job.info back to what it was
   rhoptions(write.job.info=wji)
   
   # get counters into a list of lists format
   counters <- res[[1]]$counters
   for(i in seq_along(counters)) {
      nms <- rownames(counters[[i]])
      counters[[i]] <- as.list(counters[[i]])
      names(counters[[i]]) <- nms
   }
   
   list(data=output, counters=counters)
}

#' Specify Control Parameters for RHIPE Job
#' 
#' Specify control parameters for a RHIPE job.  See \code{rhwatch} for details about each of the parameters.
#' 
#' @param mapred,setup,combiner,cleanup,orderby,shared,jarfiles,zips,jobname arguments to \code{rhwatch} in RHIPE
#' @export
rhipeControl <- function(mapred = NULL, setup = NULL, combiner = FALSE, cleanup = NULL, orderby="bytes", shared = NULL, jarfiles = NULL, zips = NULL, jobname = "") {
   res <- list(mapred = mapred, setup = setup, combiner = combiner, cleanup = cleanup, orderby = orderby, shared = shared, jarfiles = jarfiles, zips = zips, jobname = jobname)
   class(res) <- "rhipeControl"
   res
}

#' @S3method defaultControl kvHDFS
defaultControl.kvHDFS <- function(x) {
   rhipeControl()
}

