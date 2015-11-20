### general map/reduce methods

# the fundamental way to deal with divided data objects is mapreduce
# we generalize that so that if there is a new backend for divided
# data objects, we can simply implement map, reduce, and exec methods
# and then all datadr computations should work on the new backend

# map takes the input data and an expression
# which expects to have "map.keys" and "map.values" defined
# it also has a "collect" function

# reduce takes input data and an expression of pre, reduce, post
# it expects to have a "collect" function

#' Execute a MapReduce Job
#'
#' Execute a MapReduce job
#'
#' @param data a ddo/ddf object, or list of ddo/ddf objects
#' @param setup an expression of R code (created using the R command \code{expression}) to be run before map and reduce
#' @param map an R expression that is evaluated during the map stage. For each task, this expression is executed multiple times (see details).
#' @param reduce a vector of R expressions with names pre, reduce, and post that is evaluated during the reduce stage. For example \code{reduce = expression(pre = {...}, reduce = {...}, post = {...})}. reduce is optional, and if not specified the map output key-value pairs will be the result. If it is not specified, then a default identity reduce is performed. Setting it to 0 will skip the reduce altogether.
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.  If a character string, it will be treated as a path to be passed to the same type of connection as \code{data} - relative paths will be relative to the working directory of that back end.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param params a named list of objects external to the input data that are needed in the map or reduce phases
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param verbose logical - print messages about what is being done
#'
#' @return "ddo" object - to keep it simple.  It is up to the user to update or cast as "ddf" if that is the desired result.
#'
#' @author Ryan Hafen
#'
#' @export
mrExec <- function(data, setup = NULL, map = NULL, reduce = NULL, output = NULL, overwrite = FALSE, control = NULL, params = NULL, packages = NULL, verbose = TRUE) {

  # handle list of ddo/ddf - if not a list, make it one
  if(!inherits(data, "ddo")) {
    if(!all(sapply(data, function(x) inherits(x, "ddo"))))
      stop("data must be a 'ddo' or 'ddf' object or a list of these")
    # make sure all have the same storage class
    storageClasses <- sapply(data, function(x) {
      tmp <- class(x)
      tmp[grepl("^kv", tmp)][1]
    })
    uStorageClasses <- unique(storageClasses)
    if(length(uStorageClasses) != 1)
      stop("all data inputs must be of the same class - the input has data of classes ", paste(uStorageClasses, collapse = ", "))
  } else {
    data <- list(data)
  }
  class(data) <- c(paste(tail(class(data[[1]]), 1), "List", sep = ""), "list")

  # assign names to each data source if missing
  nms <- names(data)
  if(is.null(nms)) {
    nms <- paste("dataSource", seq_along(data), sep = "")
  } else {
    ind <- which(nms == "")
    if(length(ind) > 0)
      nms[ind] <- paste("unnamedDataSource", seq_along(ind), sep = "")
  }
  if(any(duplicated(nms)))
    stop("data sources must all have unique names")
  names(data) <- nms

  if(is.character(output)) {
    class(output) <- c("character", paste0(tail(class(data[[1]]), 1), "Char"))
    output <- charToOutput(output)
  }

  # TODO: make sure all data sources have same kv storage type
  mrCheckOutput(data[[1]], output)
  output <- mrCheckOutputLoc(output, as.character(overwrite))

  if(is.null(control))
    control <- list()

  # fill in missing required control fields with default
  dc <- defaultControl(data[[1]])
  controlMissingNames <- setdiff(names(dc), names(control))
  for(nm in controlMissingNames)
    control[[nm]] <- dc[[nm]]

  # if map is NULL, replace with identity
  if(is.null(map))
    map <- expression({
      for(i in seq_along(map.keys))
        collect(map.keys[[i]], map.values[[i]])
    })

  # if reduce is NULL, don't do reduce
  # but if it's a number, n, do an identity reduce with n reduce tasks
  if(is.numeric(reduce)) {
    if(reduce > 0) {
      reduce <- expression({
        reduce = {
          collect(reduce.key, reduce.values)
        }
      })
    }
  }

  mapApplyTransform <- expression({
    curTrans <- transFns[[.dataSourceName]]
    if(!is.null(curTrans)) {
      setupTransformEnv(curTrans, environment())
      for(i in seq_along(map.keys)) {
        tmp <- applyTransform(curTrans, list(map.keys[[i]], map.values[[i]]), env = environment())
        names(tmp) <- c("key", "value")
        map.keys[[i]] <- tmp[[1]]
        map.values[[i]] <- tmp[[2]]
      }
    }
  })
  map <- appendExpression(mapApplyTransform, map)

  setup <- appendExpression(control$setup, setup)
  loadPackagesSetup <- expression({
    if(length(mr___packages) > 0) {
      for(pkg in mr___packages)
        suppressMessages(require(pkg, character.only = TRUE))
    }
  })
  setup <- appendExpression(loadPackagesSetup, setup)

  setup <- nullAttributes(setup)
  map <- nullAttributes(map)
  reduce <- nullAttributes(reduce)

  # get transformations that have been added through addTransform
  transFns <- lapply(data, function(a) attr(a, "transforms")$transFns)
  params <- c(params, list(transFns = transFns))

  transPackages <- unique(do.call(c, lapply(transFns, function(a) {
    do.call(c, lapply(a, function(b) {
      b$packages
    }))
  })))

  packages <- unique(c(packages, transPackages))

  # add required packages to the list of parameters
  params <- c(params, list(mr___packages = packages))

  res <- mrExecInternal(data, setup = setup, map = map, reduce = reduce, output = output, control = control, params = params)

  obj <- ddo(res$data, update = FALSE, verbose = FALSE) # if update==TRUE, can get recursive

  # if two consecutive values are data frames with same names, chances are it's a ddf
  tmp <- try(suppressWarnings(suppressMessages(obj[1:2])), silent = TRUE)
  if(inherits(tmp, "try-error") || length(tmp) == 1) {
    tmp <- try(suppressMessages(obj[[1]]), silent = TRUE)
    if(!inherits(tmp, "try-error")) {
      if(is.data.frame(obj[[1]][[2]]))
        obj <- ddf(obj, update = FALSE, verbose = FALSE)
    }
  } else {
    if(all(sapply(tmp, function(x) inherits(x[[2]], "data.frame")))) {
      nms <- lapply(tmp, function(x) names(x[[2]]))
      if(identical(nms[[1]], nms[[2]]))
        obj <- ddf(obj, update = FALSE, verbose = FALSE)
    }
  }

  # extractableKV can change after any mr job
  obj <- setAttributes(obj, list(extractableKV = hasExtractableKV(obj), counters = res$counters))
  convert(obj, output)
}

mrExecInternal <- function(data, ...) {
  UseMethod("mrExecInternal", data)
}

defaultControl <- function(x) {
  UseMethod("defaultControl", x)
}

# check output
mrCheckOutput <- function(input, output) {
  if(!class(output)[1] %in% convertImplemented(input))
    stop("Cannot convert to requested output type")
}

mrCheckOutputLoc <- function(x, ...)
  UseMethod("mrCheckOutputLoc", x)

