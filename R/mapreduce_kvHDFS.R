### mrExec for rhData objects

#' @S3method mrExecInternal kvHDFS
mrExecInternal.kvHDFS <- function(data, setup=NULL, map=NULL, reduce=NULL, output=NULL, control=NULL, params=NULL) {
   
   if(is.null(control))
      control <- defaultControl(data)
   
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
   
   conn <- getAttribute(data, "conn")
   
   res <- rhwatch(
      setup=setup,
      map=map, 
      reduce=reduce, 
      input=rhfmt(conn$loc, type=conn$type),
      output=outFile,
      mapred=control$mapred, 
      # combiner=control$combiner,
      readback=FALSE, 
      parameters=params
   )
   
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
#' Specify control parameters for a RHIPE job.  See \code{\link{rhwatch}} for details about each of the parameters.
#' @export
rhipeControl <- function(mapred=NULL, setup=NULL, combiner=FALSE, cleanup=NULL, orderby="bytes", shared=NULL, jarfiles=NULL, zips=NULL, jobname="") {
   res <- list(mapred=mapred, setup=setup, combiner=combiner, cleanup=cleanup, orderby=orderby, shared=shared, jarfiles=jarfiles, zips=zips, jobname=jobname)
   class(res) <- "rhipeControl"
   res
}

#' @S3method defaultControl kvHDFS
defaultControl.kvHDFS <- function(x) {
   rhipeControl()
}

# #' @export
# rhText2df <- function(x, output=NULL, transFn=NULL, keyFn=NULL, linesPerBlock=10000, control=NULL, update=FALSE) {
#    if(!inherits(x, "kvHDFS"))
#       stop("Input must be data from a HDFS connection")
#    
#    if(getAttribute(x, "conn")$type != "text")
#       stop("This was designed for 'rhData' inputs of type 'text'.")
#    
#    if(is.null(transFn))
#       stop("Must specify a transformation function with 'transFn'.")
#       
#    if(is.null(control))
#       control <- defaultControl(x)
#       
#    if(is.null(control$mapred$rhipe_map_buff_size))
#       control$mapred$rhipe_map_buff_size <- linesPerBlock
#    if(is.null(control$mapred$mapred.reduce.tasks)) {
#       reduce <- 0
#    } else {
#       reduce <- control$mapred$mapred.reduce.tasks
#    }
#    
#    setup <- expression({
#       suppressWarnings(suppressMessages(require(digest)))
#    })
#    
#    map <- expression({
#       tmp <- paste(unlist(map.values), collapse="\n")
#       if(is.null(keyFn))
#          keyFn <- digest
#       
#       collect(keyFn(tmp), transFn(tmp))
#    })
#    
#    mrExec(x,
#       setup   = setup,
#       map     = map, 
#       reduce  = reduce, 
#       output  = output,
#       control = control,
#       params  = list(transFn = transFn, keyFn = keyFn)
#    )
# }
