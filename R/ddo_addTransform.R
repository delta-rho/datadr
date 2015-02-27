#' Add a Transformation Function to a Distributed Data Object
#'
#' Add a transformation function to be applied to each subset of a distributed data object
#'
#' @param obj a distributed data object
#' @param fn a function to be applied to each subset of \code{obj}
#' @param name optional name of the transformation
#' @param params a named list of parameters external to \code{obj} that are needed in the transformation function (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#'
#' @details When you add a transformation to a distributed data object, the transformation is not applied immediately, but is deferred until a function that kicks off a computation is done.  These include \code{\link{divide}}, \code{\link{recombine}}, \code{\link{drJoin}}, \code{\link{drLapply}}, \code{\link{drFilter}}, \code{\link{drSample}}, \code{drSubset}.  When any of these are invoked on an object with a transformation attached to it, the transformation will be applied in the map phase of computation prior to any other computation.  The transformation will also be applied any time a subset of the data is requested.  Thus although the data has not been physically transformed after a call of \code{addTransform}, we can think of it conceptually as already being transformed.
#' 
#' To force the transformation to be immediately calculated on all subsets use: \code{recombine(dat, combDdo, output=...)}.
#'
#' When \code{addTransform} is called, it is tested on a subset of the data to make sure we have all of the necessary global variables and packages loaded necessary to portably perform the transformation.
#'
#' It is possible to add multiple transformations to a distributed data object, in which case they are applied in the order supplied, but only one transform should be necessary.
#' 
#' The transformation function must not return NULL on any data subset,
#' although it can return an empty object of the correct shape to match other 
#' subsets (e.g. a data.frame with the correct columns but zero rows).
#' @export
addTransform <- function(obj, fn, name = NULL, params = NULL, packages = NULL) {
  if(!inherits(obj, "ddo"))
    stop("object must be a distributed data object")

  if(!is.function(fn))
    stop("argument 'fn' must be a function")

  # stip fn attributes
  attributes(fn) <- NULL

  # attach any global variables if they are needed by the function
  message("*** finding global variables used in 'fn'...", appendLF = FALSE)
  globalVarList <- drGetGlobals(fn)

  if(length(globalVarList$vars) > 0) {
    message("\n  found: ",
      paste(names(globalVarList$vars), collapse = ", "))
  } else {
    message(" [none]")
  }

  globalVarList$packages <- setdiff(globalVarList$packages, "base")
  if(length(globalVarList$packages) > 0)
    message("  package dependencies: ",
      paste(globalVarList$packages, collapse = ", "))

  # if user supplies param with same name, the first will be honored
  # so if globalVarList found the parameter, that will be used
  params <- c(globalVarList$vars, params)
  packages <- unique(c(globalVarList$packages, packages))

  # get any existing transformation functions
  # and add the new one to the list
  transFns <- attr(obj, "transforms")$transFns
  if(is.null(transFns))
    transFns <- list()

  # set function's environment to empty?

  # add function and necessary globals / packages
  idx <- length(transFns) + 1
  transFns[[idx]] <- list(fn = fn, params = params, packages = packages)
  if(is.null(name))
    name <- paste("transform", idx, sep = "")
  names(transFns)[idx] <- name
  message("*** testing 'fn' on a subset...", appendLF = FALSE)
  env <- new.env(parent = .GlobalEnv)
  environment(fn) <- env
  assign("fn", fn, envir = env)
  nms <- names(params)
  for(i in seq_along(params)) {
    if(is.function(params[[i]]))
      environment(params[[i]]) <- env

    assign(nms[i], params[[i]], envir = env)
  }
  assign("x", obj[[1]], envir = env)
  assign("kvApply", kvApply, envir = env)

  res <- try(evalq(kvApply(fn, x), envir = env))
  if(inherits(res, "try-error")) {
    stop("'fn' ran with errors on a subset:\n\n  ", geterrmessage(), "\nTo fix your transformation function, interactively test it out on a subset.  If that works, some global parameters may not have been detected.  You can specify explictly any global parameters or functions your transformation depends on through the 'params' argument.", call.=FALSE, sep = "")
  }
  message(" ok")

  classes <- class(obj)
  if(!is.data.frame(res)) {
    classes <- setdiff(classes, "ddf")
    varNames <- NULL
  } else {
    varNames <- lapply(res, class)
    # if original wasn't a ddf, make this one
    ind <- which(classes == "ddo")
    classes <- c(classes[1:ind], "ddf", classes[(ind + 1):length(classes)])
  }

  # TODO: add attributes for transformed data
  # add attributes to other
  attr(obj, "transforms") <- list(
    transFns = transFns,
    varNames = varNames
  )

  class(obj) <- c("transformed", classes)
  obj
}

loadTransformPkgs <- function(transFns) {
  allPackages <- unique(do.call(c, lapply(transFns, function(x) x$packages)))
  for(pkg in allPackages)
    suppressMessages(require(pkg, character.only = TRUE))
}

#' Set up transformation environment
#'
#' This is called internally in the map phase of datadr MapReduce jobs.  It is not meant for use outside of there, but is exported for convenience.
#' Given an environment and collection of transformations, it populates the environment with the global variables in the transformations.
#'
#' @param transFns from the "transforms" attribute of a ddo object
#' @param env the environment in which to evaluate the transformations
#' @export
setupTransformEnv <- function(transFns, env = NULL) {
  if(is.null(env))
    env <- new.env(parent = .GlobalEnv)
  for(i in seq_along(transFns)) {
    nms <- names(transFns[[i]]$params)
    for(j in seq_along(transFns[[i]]$params)) {
      if(is.function(transFns[[i]]$params[[j]]))
        environment(transFns[[i]]$params[[j]]) <- env
      assign(nms[j], transFns[[i]]$params[[j]], envir = env)
    }
  }
  env
}

#' Applies the transformation function(s)
#'
#' This is called internally in the map phase of datadr MapReduce jobs.  It is not meant for use outside of there, but is exported for convenience.
#'
#' @param transFns from the "transforms" attribute of a ddo object
#' @param x a subset of the object
#' @param env the environment in which to evaluate the function (should be instantiated from calling \code{\link{setupTransformEnv}}) - if \code{NULL}, the environment will be set up for you
#' @export
applyTransform <- function(transFns, x, env = NULL) {
  if(is.null(transFns)) {
    return(x)
  } else {
    if(is.null(env)) {
      # load required packages
      loadTransformPkgs(transFns)
      env <- setupTransformEnv(transFns)
    }
    res <- x
    # nms <- names(transFns)
    for(i in seq_along(transFns)) {
      assign("x", res, envir = env)
      curFn <- transFns[[i]]$fn
      fEnvName <- environmentName(environment(curFn))
      if(!fEnvName %in% loadedNamespaces()) {
        attributes(curFn) <- NULL
        environment(curFn) <- env
      }
      assign("fn", curFn, envir = env)
      # message(paste("*** applying transformation", nms[i]))
      res <- try(eval(expression({kvApply(fn, x, returnKV = TRUE)}), envir = env))
      if(inherits(res, "try-error"))
        stop("attempt to apply transformation failed with error: ", geterrmessage(), call. = FALSE)
        # TODO: add information about key to error message
      # res <- kvApply(curFn, res, returnKV = TRUE)
    }
    attr(res[[2]], "split") <- attr(x[[2]], "split")
    res
  }
}

# addFilter?
# addSample?



# # evaluate each function in an environment with required variables
# for(i in seq_along(transFns)) {
#   env <- new.env(parent = .GlobalEnv)
#   fEnvName <- environmentName(environment(transFns[[i]]$fn))
#   if(!fEnvName %in% loadedNamespaces()) {
#     attributes(transFns[[i]]$fn) <- NULL
#     environment(transFns[[i]]$fn) <- env
#   }
#   assign("fn", transFns[[i]]$fn, envir = env)
#   nms <- names(transFns[[i]]$params)
#   for(j in seq_along(transFns[[i]]$params)) {
#     if(is.function(transFns[[i]]$params[[j]]))
#       environment(transFns[[i]]$params[[j]]) <- env
#     assign(nms[j], transFns[[i]]$params[[j]], envir = env)
#   }
#   assign("x", x, envir = env)
#   assign("kvApply", kvApply, envir = env)
#   res <- try(eval(expression({kvApply(fn, x, returnKV = TRUE)}), envir = env))
#   if(inherits(res, "try-error"))
#     stop("attempt to apply transformation failed with error: ", geterrmessage(), call. = FALSE)
# }
