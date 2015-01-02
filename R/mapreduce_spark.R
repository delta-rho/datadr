### mrExec for sparkData objects

#' @export
mrExecInternal.kvSparkDataList <- function(data, setup = NULL, map = NULL, reduce = NULL, output = NULL, control = NULL, params = NULL) {

  if(length(data) > 1)
    stop("Currently can't handle more than one set of inputs when using Spark as the back end.")

  data <- data[[1]]

  conn <- getAttribute(data, "conn")
  rdd <- conn$data

  # "map" currently conflicts with SparkR's map()
  mapExp <- map
  reduceExp <- reduce

  paramsBr <- broadcast(getSparkContext(), params)

  rddMap <- function(kv) {
    params <- value(paramsBr)
    pnames <- names(params)
    for(i in seq_along(params)) {
      if(is.function(params[[i]]))
        environment(params[[i]]) <- environment()
      assign(pnames[i], params[[i]], envir = environment())
    }
    assign(".dataSourceName", "data", envir = environment())

    eval(setup, envir = environment())

    map.keys <- lapply(kv, "[[", 1)
    map.values <- lapply(kv, "[[", 2)
    taskRes <- list()

    collect <- function(k, v) {
      taskRes[[length(taskRes) + 1]] <<- list(k, v)
    }
    counter <- function(group, field, ct) NULL

    eval(mapExp, envir = environment())
    return(taskRes)
  }

  rddReduce <- function(kv) {
    params <- value(paramsBr)
    pnames <- names(params)
    for(i in seq_along(params)) {
      if(is.function(params[[i]]))
        environment(params[[i]]) <- environment()
      assign(pnames[i], params[[i]], envir = environment())
    }

    reduce.key <- kv[[1]]
    reduce.values <- kv[[2]]
    # return(list(reduce.key = reduce.key, reduce.values = reduce.values))
    taskRes <- list()
    collect <- function(k, v) {
      taskRes[[length(taskRes) + 1]] <<- list(k, v)
    }
    counter <- function(group, field, ct) NULL

    eval(setup, envir = environment())
    eval(reduceExp$pre, envir = environment())

    # a real reduce would allow to iterate over blocks of
    # reduce.values for a given reduce.key, if there the
    # reduce.values are much larger than will fit in memory
    eval(reduceExp$reduce, envir = environment())

    eval(reduceExp$post, envir = environment())

    return(unlist(taskRes, recursive = FALSE))
  }
  # rddMap(rdd)

  mr <- lapplyPartition(rdd, rddMap)
  gr <- groupByKey(mr, 1L)
  rr <- map(gr, rddReduce)
  # res <- collect(rr)
  res <- persist(rr, "MEMORY_ONLY")
  # NOTE: see setMethod("groupByKey" ...
  #  perhaps we can save computation by just doing partitionBy

  # TODO: once accumulators have been implemented, get counters

  if(inherits(output, "sparkDataConn")) {
    saveAsObjectFile(res, paste(output$hdfsURI, output$loc, "/data0", sep = ""))
    res <- output
  } else {
    res <- collect(res)
  }

  list(data = res, counters = NULL)
}

#' Specify Control Parameters for Spark Job
#'
#' Specify control parameters for a Spark job.  See \code{rhwatch} for details about each of the parameters.
#' @export
sparkControl <- function() {
  NULL
}

#' @export
defaultControl.kvSparkData <- function(x) {
  sparkControl()
}
