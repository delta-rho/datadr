### mrExec for sparkData objects

#' @S3method mrExecInternal kvSparkDataList
mrExecInternal.kvSparkDataList <- function(data, setup=NULL, map=NULL, reduce=NULL, output=NULL, control=NULL, params=NULL) {
   
   dat <- unlist(lapply(data, function(x) {
      conn <- getAttribute(x, "conn")
      conn$data
   }), recursive = FALSE)
   
   conn <- getAttribute(data[[1]], "conn")
   sc <- do.call(sparkR.init, c(conn$init, list(appName = "datadrMR")))
   rdd <- parallelize(sc, dat)
   
   # "map" currently conflicts with SparkR's map()
   mapExp <- map
   reduceExp <- reduce
   
   paramsBr <- broadcast(sc, params)
   
   rddMap <- function(kv) {
      params <- value(paramsBr)
      pnames <- names(params)
      for(i in seq_along(params)) {
         if(is.function(params[[i]]))
            environment(params[[i]]) <- environment()
         assign(pnames[i], params[[i]], envir = environment())
      }
      
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
      
      return(taskRes)
   }
   # rddMap(conn$data)
   
   mr <- lapplyPartition(rdd, rddMap)
   gr <- groupByKey(mr, 1L)
   rr <- lapply(gr, rddReduce)
   res <- collect(rr)
   res <- unlist(res, recursive = FALSE)
   # NOTE: see setMethod("groupByKey" ...
   #   perhaps we can save computation by just doing partitionBy
   
   # TODO: once accumulators have been implemented, get counters
   
   list(data = sparkDataConn(data = res, init = data$init), counters = NULL)
}

#' Specify Control Parameters for Spark Job
#' 
#' Specify control parameters for a Spark job.  See \code{rhwatch} for details about each of the parameters.
#' @export
sparkControl <- function() {
   NULL
}

#' @S3method defaultControl kvSparkData
defaultControl.kvSparkData <- function(x) {
   sparkControl()
}

# # all the env stuff probably isn't necessary
# rddMap <- function(kv) {
#    mapEnv <- new.env()
#    assign("map.keys", lapply(kv, "[[", 1), mapEnv)
#    assign("map.values", lapply(kv, "[[", 2), mapEnv)
#    assign("taskRes", list(), mapEnv)
#    
#    spCollect <- function(k, v) {
#       taskRes[[length(taskRes) + 1]] <<- list(k, v)
#    }
#    environment(spCollect) <- mapEnv
#    assign("collect", spCollect, mapEnv)
#    
#    spCounter <- function(group, field, ct) NULL
#    environment(spCounter) <- mapEnv
#    assign("counter", spCounter, mapEnv)
#    
#    # eval(setup, envir = mapEnv)
#    # for(i in seq_along(params)) {
#    #    if(is.function(params[[i]]))
#    #       environment(params[[i]]) <- mapEnv
#    # }
#    
#    eval(mapExp, envir = mapEnv)
#    return(mapEnv$taskRes)
# }