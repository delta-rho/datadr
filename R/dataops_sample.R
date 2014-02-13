
#' Take a Sample of Key-Value Pairs
#' @export
drSample <- function(x, fraction) {
   # TODO: warn if output storage is not commensurate with input?
   map <- expression({
      for(i in seq_along(map.keys)) {
         if(runif(1) < fraction)
            collect(map.keys[[i]], map.values[[i]])
      }
   })
   
   mrExec(inputs,
      map = map,
      control = control,
      output = output,
      params = list(fraction = fraction)
   )
}
