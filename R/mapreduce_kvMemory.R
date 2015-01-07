## mrExec for kvMemory objects

#' @export
mrExecInternal.kvMemoryList <- function(data, setup=NULL, map=NULL, reduce=NULL, output=NULL, control=NULL, params=NULL) {

  # set up empty environment for map and reduce expressions to be evaluated in
  mapEnv <- new.env() # parent = baseenv())
  reduceEnv <- new.env() # parent = baseenv())

  # add any needed data objects to map and reduce environments
  if(!is.null(params)) {
    pnames <- names(params)
    for(i in seq_along(params)) {
      if(is.function(params[[i]]))
        environment(params[[i]]) <- mapEnv
      assign(pnames[i], params[[i]], envir=mapEnv)
      assign(pnames[i], params[[i]], envir=reduceEnv)
    }
  }

  eval(setup, envir=mapEnv)
  eval(setup, envir=reduceEnv)

  ### do the map
  assign("counterRes", list(), mapEnv)
  assign("mapRes", list(), mapEnv)
  eval(expression({
    collect <- function(k, v) {
      mapRes[[length(mapRes) + 1]] <<- list(k, v)
    }

    counter <- function(group, field, ct) {
      if(is.null(counterRes[[group]]))
        counterRes[[group]] <<- list()
      if(is.null(counterRes[[group]][[field]]))
        counterRes[[group]][[field]] <<- 0
      counterRes[[group]][[field]] <<- counterRes[[group]][[field]] + ct
    }
  }), envir=mapEnv)

  # loop through inputs
  nms <- names(data)
  for(i in seq_along(data)) {
    kvData <- getAttribute(data[[i]], "conn")$data
    assign("map.keys", lapply(kvData, "[[", 1), mapEnv)
    assign("map.values", lapply(kvData, "[[", 2), mapEnv)
    assign(".dataSourceName", nms[i], mapEnv)
    eval(map, envir=mapEnv)
  }

  if(!is.null(reduce)) {
    reduce.keys <- lapply(get("mapRes", mapEnv), "[[", 1)
    reduce.values <- lapply(get("mapRes", mapEnv), "[[", 2)
    reduce.digKeys <- sapply(reduce.keys, digest)
    reduce.uDigKeys <- unique(reduce.digKeys)
    reduce.uKeys <- reduce.keys[which(!duplicated(reduce.digKeys))]

    assign("counterRes", get("counterRes", mapEnv), reduceEnv)
    assign("reduceRes", list(), reduceEnv)
    assign("reduce.uDigKeys", reduce.uDigKeys, reduceEnv)
    assign("reduce.uKeys", reduce.uKeys, reduceEnv)
    assign("reduce.allValues", reduce.values, reduceEnv)
    eval(expression({
      collect <- function(k, v) {
        reduceRes[[length(reduceRes) + 1]] <<- list(k, v)
      }

      counter <- function(group, field, ct) {
        if(is.null(counterRes[[group]]))
          counterRes[[group]] <<- list()
        if(is.null(counterRes[[group]][[field]]))
          counterRes[[group]][[field]] <<- 0
        counterRes[[group]][[field]] <<- counterRes[[group]][[field]] + ct
      }
    }), envir=reduceEnv)

    for(i in seq_along(reduce.uKeys)) {
      assign("reduce.key", reduce.uKeys[[i]], reduceEnv)
      assign("curValIdx", which(reduce.digKeys==reduce.uDigKeys[i]), reduceEnv)
      eval(expression({
        reduce.values <- reduce.allValues[curValIdx]
      }), envir=reduceEnv)

      eval(reduce$pre, envir=reduceEnv)
      eval(reduce$reduce, envir=reduceEnv)
      eval(reduce$post, envir=reduceEnv)
    }
    res <- get("reduceRes", envir=reduceEnv)
    counters <- get("counterRes", envir=reduceEnv)
  } else {
    res <- get("mapRes", envir=mapEnv)
    counters <- get("counterRes", envir=mapEnv)
  }

  list(data=res, counters=counters)
}

#' @export
defaultControl.kvMemory <- function(x) {
  NULL
}



