#' @export
rhText2DF <- function(x, output=NULL, trans=NULL, keyFn=NULL, control=NULL, update=FALSE) {
   
   if(is.null(trans))
      stop("Must specify a transformation function with 'trans'.")
      
   if(is.null(control))
      control <- defaultControl(data)
      
   if(x$type != "text")
      stop("This was designed for 'rhData' inputs of type 'text'.")
      
   map <- expression({
      tmp <- paste(unlist(map.values), collapse="\n")
      if(is.null(keyFn))
         keyFn <- digest
      
      rhcollect(keyFn(tmp), trans(tmp))
   })
   
   setup <- expression({
      suppressWarnings(suppressMessages(require(digest)))
      .libPaths(libPaths)
   })
   
   setup <- appendExpression(control$setup, setup)
   
   if(is.null(control$mapred$mapred.reduce.tasks)) {
      reduce <- 0
   } else {
      reduce <- control$mapred$mapred.reduce.tasks
   }
   
   # set write.job.info to TRUE
   wji <- rhoptions()$write.job.info
   rhoptions(write.job.info=TRUE)
   
   tmp <- rhwatch(
      setup=nullAttributes(setup),
      map=nullAttributes(map), 
      reduce=nullAttributes(reduce), 
      input=rhfmt(x$loc, type=x$type),
      output=rhfmt(output, type="sequence"),
      mapred=control$mapred, 
      combiner=control$combiner,
      readback=FALSE, 
      parameters=list(
         trans = trans,
         keyFn = keyFn,
         libPaths = .libPaths()
      )
   )
   
   # set write.job.info back to what it was
   rhoptions(write.job.info=wji)
   
   rhDF(output, type="sequence", update=update)
}
