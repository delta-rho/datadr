if(getRversion() >= "2.15.1") {
  utils::globalVariables(
      c("taskDir", "task_id", "taskCounter", "taskRes",
         "writeKVseparately", "countersDir", "reduce.key",
         "obj"
      )
   )
}

## mrExec for kvLocalDisk objects

#' @export
mrExecInternal.kvLocalDiskList <- function(data, setup = NULL, map = NULL, reduce = NULL, output = NULL, control = NULL, params = NULL) {
   setup <- appendExpression(setup,
      expression({
         suppressWarnings(suppressMessages(require(digest)))
      })
   )
   if(is.null(reduce))
      reduce <- expression(pre = {}, reduce = {collect(reduce.key, reduce.values)}, post = {})

   nSlots <- 1
   if(!is.null(control$cluster))
      nSlots <- length(control$cluster)

   # intermediate map and reduce results will go in a temporary directory
   if(is.null(control$mapred_temp_dir)) {
      tempDir <- tempdir()
   } else {
      tempDir <- control$mapred_temp_dir
   }
   # put results in a "job" directory named "job_i" where i increments
   jobDirs <- list.files(tempDir, pattern = "^job_")
   jobNums <- as.integer(gsub("^job_", "", jobDirs))
   if(length(jobNums) == 0) {
      jobNum <- 1
   } else {
      jobNum <- max(jobNums) + 1
   }
   jobDir <- file.path(tempDir, paste("job_", jobNum, sep = ""))
   stopifnot(dir.create(jobDir))
   # add directory for map, counters, log
   mapDir <- file.path(jobDir, "map")
   reduceDir <- file.path(jobDir, "reduce")
   countersDir <- file.path(jobDir, "counters")
   logDir <- file.path(jobDir, "log")
   stopifnot(dir.create(mapDir))
   stopifnot(dir.create(reduceDir))
   stopifnot(dir.create(countersDir))
   stopifnot(dir.create(logDir))

   ### map task setup
   # get file names and use file size to determine how to allocate

   # split ff into nSlots chunks with roughly equal size
   nms <- names(data)
   mFileList <- lapply(seq_along(data), function(i) {
      conn <- getAttribute(data[[i]], "conn")
      fp <- conn$loc
      ff <- getAttribute(data[[i]], "files")
      sz <- getAttribute(data[[i]], "sizes")

      nFile <- length(ff)
      idx <- makeBlockIndices(sz, sum(sz) / nSlots, nSlots)

      lapply(idx, function(x) {
         list(fp = fp, ff = ff[x], sz = sz[x], dataSourceName = nms[i])
      })
   })
   mFileList <- unlist(mFileList, recursive = FALSE)

   # give a map task id to each block
   for(i in seq_along(mFileList)) {
      mFileList[[i]]$map_task_id <- i
   }
   # sum(sapply(mFileList, function(x) length(x$ff)))
   # sapply(mFileList, function(x) sum(x$sz))

   mapFn <- function(fl) {
      mapEnv <- new.env() # parent = baseenv())
      assign("mr___packages", params$mr___packages, mapEnv)
      eval(setup, envir = mapEnv)

      # add a collect, counter functions to the environment
      environment(LDcollect) <- mapEnv
      environment(LDcounter) <- mapEnv
      environment(LDflushKV) <- mapEnv
      assign("collect", LDcollect, mapEnv)
      assign("counter", LDcounter, mapEnv)
      assign("flushKV", LDflushKV, mapEnv)
      assign("REDUCE", FALSE, mapEnv)

      ### do the map
      assign("task_id", fl$map_task_id, mapEnv)
      assign("taskDir", mapDir, mapEnv)
      assign("taskRes", list(), mapEnv)
      assign("taskCounter", list(), mapEnv)
      assign("writeKVseparately", TRUE, mapEnv) # more efficient
      assign(".dataSourceName", fl$dataSourceName, mapEnv)

      for(i in seq_along(params)) {
         if(is.function(params[[i]]))
            environment(params[[i]]) <- mapEnv
      }

      # iterate through map blocks and apply map to each
      mapBlocks <- makeBlockIndices(fl$sz, control$map_buff_size_bytes, nSlots)
      for(idx in mapBlocks) {
         # set fresh params for each application of map expression
         # in case a previous map updates them
         pnames <- names(params)
         for(i in seq_along(params))
            assign(pnames[i], params[[i]], envir = mapEnv)

         curDat <- lapply(fl$ff[idx], function(x) {
            load(file.path(fl$fp, x))
            obj[[1]]
         })
         assign("map.keys", lapply(curDat, "[[", 1), mapEnv)
         assign("map.values", lapply(curDat, "[[", 2), mapEnv)
         eval(expression({
            .tmp <- environment()
            attach(.tmp, warn.conflicts = FALSE, name = "custom .tmp with detach two lines later")
         }), envir = mapEnv)
         eval(map, envir = mapEnv)
         eval(expression({detach("custom .tmp with detach two lines later")}), envir = mapEnv)

         # count number of k/v processed
         evalq(counter("map", "kvProcessed", length(map.values)), mapEnv)

         # cat(evalq(object.size(taskRes), mapEnv), "\n")
         if(evalq(object.size(taskRes), mapEnv) > control$map_temp_buff_size_bytes)
            evalq(flushKV(), envir = mapEnv)
      }
      evalq(flushKV(), envir = mapEnv)
   }

   ### run map tasks
   if(!is.null(control$cluster)) {
      clusterExport(control$cluster, c("map", "reduce", "setup", "params", "mapDir", "reduceDir", "countersDir", "makeBlockIndices", "nSlots", "control", "LDflushKV", "LDcounter", "LDcollect", "params"), envir = environment())

      parLapply(control$cluster, mFileList, mapFn)
   } else {
      lapply(mFileList, mapFn)
   }

   ### reduce task setup
   # figure out how to divvy up reduce tasks
   # (ideally should use in-memory size, not file size)
   rmf <- list.files(mapDir)
   rf <- lapply(rmf, function(x) {
      ff <- list.files(file.path(mapDir, x), recursive = TRUE, pattern = "value")
      list(
         ff = ff,
         sz = file.info(file.path(mapDir, x, ff))$size,
         fp = file.path(mapDir, x)
      )
   })

   sz <- sapply(rf, function(x) sum(x$sz))
   idx <- makeBlockIndices(sz, sum(sz) / nSlots, nSlots)

   # each element of rFileList is a list of reduce keys and associated data
   rFileList <- lapply(seq_along(idx), function(i) {
      list(
         rf = rf[idx[[i]]],
         reduce_task_id = i
      )
   })

   reduceFn <- function(reduceTaskFiles) {
      reduceEnv <- new.env() # parent = baseenv())
      if(!is.null(params)) {
         pnames <- names(params)
         for(i in seq_along(params)) {
            if(is.function(params[[i]]))
               environment(params[[i]]) <- reduceEnv
            assign(pnames[i], params[[i]], envir = reduceEnv)
         }
      }
      eval(setup, envir = reduceEnv)

      # add collect, counter functions to the environment
      environment(LDcollect) <- reduceEnv
      environment(LDcounter) <- reduceEnv
      environment(LDflushKV) <- reduceEnv
      assign("collect", LDcollect, reduceEnv)
      assign("counter", LDcounter, reduceEnv)
      assign("flushKV", LDflushKV, reduceEnv)
      assign("REDUCE", TRUE, reduceEnv)

      ### do the reduce
      assign("task_id", reduceTaskFiles$reduce_task_id, reduceEnv)
      assign("taskDir", reduceDir, reduceEnv)
      assign("writeKVseparately", FALSE, reduceEnv) # more efficient

      for(curReduceFiles in reduceTaskFiles$rf) {
         assign("taskRes", list(), reduceEnv)
         assign("taskCounter", list(), reduceEnv)

         load(list.files(curReduceFiles$fp, recursive = TRUE, pattern = "key\\.Rdata", full.names = TRUE)[1])
         assign("reduce.key", k, reduceEnv)

         # nSlots is 1 because we are already in parLapply
         reduceBlocks <- makeBlockIndices(curReduceFiles$sz, control$reduce_buff_size_bytes, nSlots = 1)
         eval(reduce$pre, envir = reduceEnv)

         for(idx in reduceBlocks) {
            curDat <- do.call(c, lapply(curReduceFiles$ff[idx], function(x) {
               load(file.path(curReduceFiles$fp, x))
               v
            }))
            assign("reduce.values", curDat, reduceEnv)
            eval(reduce$reduce, envir = reduceEnv)
         }
         eval(reduce$post, envir = reduceEnv)
         # count number of k/v processed
         evalq(counter("reduce", "kvProcessed", 1), reduceEnv)
         evalq(flushKV(), envir = reduceEnv)
      }
   }

   ### run reduce tasks
   if(!is.null(control$cluster)) {
      parLapply(control$cluster, rFileList, reduceFn)
   } else {
      lapply(rFileList, reduceFn)
   }

   ### take care of results
   outputKeyHash <- list.files(reduceDir)

   # if output is a character string, construct a localDiskConn connection from it
   if(!inherits(output, "localDiskConn")) {
      if(is.character(output)) {
         output <- localDiskConn(output, nBins = floor(length(outputKeyHash) / 1000), verbose = FALSE)
      } else {
         output <- localDiskConn(tempfile(paste("job", jobNum, "_", sep = "")), nBins = floor(length(outputKeyHash) / 1000), autoYes = TRUE, verbose = FALSE)
      }
   }

   # move it to the output directory
   # if there is only one .Rdata file in each of these
   # we can simply move them to the destination
   # otherwise, we need to read them in and combine them first
   # UPDATE: since we are allowing custom hash functions, for now,
   # we just read in each and write it back out according to the appropriate file hash
   # TODO: move this into the map and reduce so we don't have to do it here
   for(x in outputKeyHash) {
      ff <- list.files(file.path(reduceDir, x), recursive = TRUE, full.names = TRUE)
      # if(length(ff) == 1) {
      #    newFile <- getFileLocs(output, x)
      #    file.rename(ff, newFile)
      # } else {
         tmp <- do.call(c, lapply(ff, function(x) {
            load(x)
            obj
         }))
         addData(output, tmp)
      # }
   }

   # read counters
   groupf <- list.files(countersDir, full.names = TRUE)
   counters <- lapply(groupf, function(f) {
      fieldf <- list.files(f, full.names = TRUE)
      fieldNames <- basename(fieldf)

      tmp <- lapply(fieldf, function(a) {
         sum(as.integer(basename(list.files(a, recursive = TRUE))))
      })
      names(tmp) <- fieldNames
      tmp
   })
   names(counters) <- basename(groupf)

   # TODO: add input, map, reduce, etc. to _meta

   # clean up (leave counters and log)
   unlink(mapDir, recursive = TRUE)
   unlink(reduceDir, recursive = TRUE)
   file.create(file.path(jobDir, "SUCCESS"))

   list(data = output, counters = counters)
}

# take a vector of sizes and return a list of indices where
# the sum of sizes for each collection of indices is roughly sizePerBlock
# (used to create "buffers" of data sent to map and reduce tasks)
# also take number of cores available into account
makeBlockIndices <- function(sz, sizePerBlock, nSlots = 1) {
   cs <- cumsum(sz)
   # number of blocks to split files into
   n <- max(ceiling(sum(sz) / sizePerBlock), nSlots)
   # if there are fewer files than slots:
   n <- min(length(cs), n)
   if(n == 1) {
      return(list(seq_along(sz)))
   } else {
      qs <- quantile(cumsum(sz), seq(0, 1, length = n + 1))
      res <- split(seq_along(sz), cut(cumsum(sz), qs, include.lowest = TRUE))
      names(res) <- NULL
      return(res)
   }
}

#' Specify Control Parameters for MapReduce on a Local Disk Connection
#'
#' Specify control parameters for a MapReduce on a local disk connection.  Currently the parameters include:
#' @param cluster a "cluster" object obtained from \code{\link{makeCluster}} to allow for parallel processing
#' @param map_buff_size_bytes determines how much data should be sent to each map task
#' @param reduce_buff_size_bytes determines how much data should be sent to each reduce task
#' @param map_temp_buff_size_bytes determines the size of chunks written to disk in between the map and reduce
#' @note If you have data on a shared drive that multiple nodes can access or a high performance shared file system like Lustre, you can run a local disk MapReduce job on multiple nodes by creating a multi-node cluster with \code{\link{makeCluster}}.
#'
#' If you are using multiple cores and the input data is very small, \code{map_buff_size_bytes} needs to be small so that the key-value pairs will be split across cores.
#' @export
localDiskControl <- function(
   cluster = NULL,
   map_buff_size_bytes = 10485760,
   reduce_buff_size_bytes = 10485760,
   map_temp_buff_size_bytes = 10485760
) {
   structure(list(
      cluster = cluster,
      map_buff_size_bytes = map_buff_size_bytes,
      reduce_buff_size_bytes = reduce_buff_size_bytes,
      map_temp_buff_size_bytes = map_temp_buff_size_bytes
   ), class = c("localDiskControl", "list"))
}

#' @export
defaultControl.kvLocalDisk <- function(x) {
   res <- getOption("defaultLocalDiskControl")
   if(inherits(res, "localDiskControl")) {
      return(res)
   } else {
      return(localDiskControl())
   }
}

# this will be used for both map and reduce
LDcollect <- function(k, v) {
   # each unique key emitted from map has its data stored in its own directory
   # (name determined by digest)
   dk <- digest(k)
   # also create a subdirectory named task_id to avoid conflicts
   kPath <- file.path(taskDir, dk, task_id)
   if(!file.exists(kPath))
      dir.create(kPath, recursive = TRUE)

   # if this is the first, store key in a file key.Rdata
   # then build up a list of map results until it's too big (then flush to disk)
   if(is.null(taskCounter[[dk]])) {
      taskCounter[[dk]] <<- 1
      taskRes[[dk]] <<- list()
      if(writeKVseparately)
         save(k, file = file.path(kPath, "key.Rdata"))
   } else {
      taskCounter[[dk]] <<- taskCounter[[dk]] + 1
   }
   taskRes[[dk]][[length(taskRes[[dk]]) + 1]] <<- v
}

# counter works by incrementing a filename
LDcounter <- function(group, field, ct) {
   countersPath <- file.path(countersDir, group, field, task_id)
   if(!file.exists(countersPath)) {
      dir.create(countersPath, recursive = TRUE)
      file.create(file.path(countersPath, "0"))
   }
   curCt <- as.numeric(list.files(countersPath))
   newCt <- curCt + ct
   file.rename(file.path(countersPath, curCt), file.path(countersPath, newCt))
}

LDflushKV <- function() {
   fKeys <- names(taskRes)
   for(i in seq_along(fKeys)) {
      kPath <- file.path(taskDir, fKeys[i], task_id)
      fmf <- list.files(kPath, pattern = "^value")
      fmf <- as.integer(gsub("value(.*)\\.Rdata", "\\1", fmf))
      if(length(fmf) == 0) {
         fileIdx <- 1
      } else {
         fileIdx <- max(fmf) + 1
      }
      if(writeKVseparately) {
         v <- taskRes[[fKeys[i]]]
         save(v, file = file.path(kPath, paste("value", fileIdx, ".Rdata", sep = "")))
      } else {
         # if there is more than one value element
         # each needs to be part of a k/v pair
         obj <- lapply(taskRes[[fKeys[i]]], function(x) {
            list(reduce.key, x)
         })
         save(obj, file = file.path(kPath, paste("value", fileIdx, ".Rdata", sep = "")))
      }

      # now reset results for this key
      taskRes[[fKeys[i]]] <<- NULL
      taskCounter[[fKeys[i]]] <<- NULL
   }
}


