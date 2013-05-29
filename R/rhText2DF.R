#' @export
rhText2DF <- function(x, output=NULL, trans=NULL, control=NULL, update=FALSE) {
   
   if(is.null(trans))
      stop("Must specify a transformation function with 'trans'.")
      
   if(is.null(control))
      control <- defaultControl(data)
      
   if(x$type != "text")
      stop("This was designed for 'rhData' inputs of type 'text'.")
      
   map <- expression({
      tmp <- paste(unlist(map.values), collapse="\n")
      rhcollect(digest(tmp), trans(tmp))
   })
   
   setup <- expression({
      suppressMessages(require(digest))
   })
   
   setup <- appendExpression(control$setup, setup)
   
   # set write.job.info to TRUE
   wji <- rhoptions()$write.job.info
   rhoptions(write.job.info=TRUE)
   
   control$mapred$mapred.reduce.tasks <- 0
   
   tmp <- rhwatch(
      setup=nullAttributes(setup),
      map=nullAttributes(map), 
      input=rhfmt(x$loc, type=x$type),
      output=rhfmt(output, type="sequence"),
      mapred=control$mapred, 
      combiner=control$combiner,
      readback=FALSE, 
      parameters=list(
         trans = trans
      )
   )
   
   # set write.job.info back to what it was
   rhoptions(write.job.info=wji)
   
   rhDF(output, type="sequence", update=update)
}
