# note: drFindGlobals and getGlobalVarList are used both in datadr and trelliscope
# I don't want to export them, so they are defined in both places
# example usage:
# globalVars <- findGlobals(panelFn)
# globalVarList <- getGlobalVarList(globalVars, parent.frame())

## internal
drFindGlobals <- function(f) {
   on.exit({
      try(assign("~", tildeHandler, envir = codetools:::collectUsageHandlers), silent = TRUE)
   })
   
   if(!is.function(f)) 
      return(character(0))
   
   tildeHandler <- codetools:::collectUsageHandlers[["~"]]
   try(remove("~", envir = codetools:::collectUsageHandlers), silent = TRUE)
   res <- try(codetools::findGlobals(f, merge = FALSE)$variables, silent = TRUE)
   if(inherits(res, "try-error"))
      res <- NULL
   res
}

getGlobalVarList <- function(globalVars, parentFrame) {
   globalVarList <- list()
   # look first on parent frame
   tmp <- intersect(globalVars, ls(envir = parentFrame))
   if(length(tmp) > 0) {
      for(i in seq_along(tmp)) {
         globalVarList[[tmp[i]]] <- get(tmp[i], parentFrame)
      }
   }
   # then look through all others
   for(env in search()) {
      tmp <- intersect(globalVars, ls(envir=as.environment(env)))
      if(length(tmp) > 0) {
         for(i in seq_along(tmp)) {
            if(is.null(globalVarList[[tmp[i]]]))
               globalVarList[[tmp[i]]] <- get(tmp[i], as.environment(env))
         }
      }
   }
   globalVarList
}
