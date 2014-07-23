
#' @export
drGetGlobals <- function(f) {
   if(!is.function(f))
      return(NULL)
   
   ## this is meant to deal with variables in formulae
   ## but it's too dangerous
   # on.exit({
   #    try(assign("~", tildeHandler, envir = codetools:::collectUsageHandlers), silent = TRUE)
   # })
   # tildeHandler <- codetools:::collectUsageHandlers[["~"]]
   # try(remove("~", envir = codetools:::collectUsageHandlers), silent = TRUE)
   
   fEnvName <- environmentName(environment(f))
   if(fEnvName %in% loadedNamespaces()) {
      # put fEnvName in list of packages
      # then all we need is the function - a package function
      # shouldn't have any global variable dependencies
      return(list(packages = fEnvName, vars = NULL))
   } else {
      res <- getGlobalPkgVars(f)
      
      # get all functions and see if they also have global dependencies
      if(length(res$vars) > 0) {
         fnInd <- which(sapply(res$vars, is.function))

         curVars <- res$vars
         while(length(fnInd) > 0) {
            varList <- NULL
            for(f in curVars[fnInd]) {
               tmp <- getGlobalPkgVars(f)

               # merge in result...
               if(length(tmp$vars) > 0) {
                  newNames <- setdiff(names(tmp$vars), names(res$vars))
                  varList <- c(varList, tmp$vars[newNames])
               }

               if(length(tmp$packages) > 0) {
                  res$packages <- unique(c(res$packages, tmp$packages))
               }
            }
            if(length(varList) > 0) {
               fnInd <- which(sapply(varList, is.function))
               res$vars <- c(res$vars, varList)
            } else {
               fnInd <- NULL
            }
            curVars <- varList
         }
      }
      if(length(res$vars) == 0)
         res["vars"] <- list(NULL)
      return(res)
   }
}

getGlobalPkgVars <- function(f) {
   # first see if function is part of a package
   # if so, we shouldn't need to do anything
   # (except add that package to the packages list, of course)
   
   # get list of names of globals used in functions
   res <- try(codetools::findGlobals(f), silent = TRUE)
   if(inherits(res, "try-error"))
      res <- NULL
   
   # first search through call stack and grab variables
   # message(environmentName(environment(f)))
   vars <- getGlobalVars(res, environment(f))
   # now see if what is leftover can be accounted for in packages
   left <- setdiff(res, names(vars))
   pkgs <- getPackages(left)
   
   # now there shouldn't be anything left...
   left <- setdiff(left, pkgs$accounted)
   # if(length(left) > 0)
   #    message("* warning: could not find global variables: ", paste(left, collapse = ", "), sep = "")
   
   list(vars = vars, packages = pkgs$packages)
}

getGlobalVars <- function(globalVars, startEnv) {
   if(!is.environment(startEnv))
      startEnv <- .GlobalEnv
   
   globalVarList <- list()
   
   # step through call stack until we get to global environment
   # if there are multiple variables of same name
   # keep the one that is closest to function environment
   curEnv <- startEnv
   repeat {
      tmp <- intersect(globalVars, ls(envir = curEnv))
      for(i in seq_along(tmp)) {
         if(is.null(globalVarList[[tmp[i]]])) {
            val <- get(tmp[i], curEnv)
            if(is.null(val)) {
               # deal with NULL removing from list
               globalVarList[tmp[i]] <- list(NULL)
            } else {
               globalVarList[[tmp[i]]] <- val
            }
         }
      }
      
      curEnvName <- environmentName(curEnv)
      if(curEnvName == "R_GlobalEnv" || curEnvName == "R_EmptyEnv")
         break
      curEnv <- parent.env(curEnv)
   }
   globalVarList   
}

getPackages <- function(globalVars) {
   pkgs <- search()
   pkgs <- pkgs[grepl("^package:", pkgs)]
   
   globalPkgList <- NULL
   accounted <- NULL
   
   for(pkg in pkgs) {
      tmp <- intersect(globalVars, ls(envir = as.environment(pkg)))
      
      if(length(tmp) > 0) {
         accounted <- c(accounted, tmp)
         globalPkgList <- c(globalPkgList, gsub("^package:", "", pkg))
      }
   }
   
   list(packages = globalPkgList, accounted = accounted)
}


