#' Recombine
#'
#' Apply an analytic recombination method to a ddo/ddf object and combine the results
#'
#' @param data an object of class "ddo" of "ddf"
#' @param apply a function specifying the analytic method to apply to each subset, or a pre-defined apply function (see \code{\link{drBLB}}, \code{\link{drGLM}}, for example)
#' @param combine the method to combine the results
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param verbose logical - print messages about what is being done
#'
#' @return depends on \code{combine}
#'
#' @references
#' \itemize{
#'  \item \url{http://www.datadr.org}
#'  \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#'
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{drGLM}}, \code{\link{drBLB}}, \code{\link{combMeanCoef}}, \code{\link{combMean}}, \code{\link{combCollect}}, \code{\link{combRbind}}, \code{\link{drLapply}}
#' @export
recombine <- function(data, combine = NULL, apply = NULL, output = NULL, overwrite = FALSE, params = NULL, packages = NULL, control = NULL, verbose = TRUE) {

  if(is.null(combine)) {
    if(is.null(output)) {
      combine <- combCollect()
    } else {
      combine <- combDdo()
    }
  } else if(is.function(combine)) {
    combine <- combine()
  }

  if(!is.null(apply)) {
    message("** note **: 'apply' argument is deprecated - please apply this transformation using 'addTransform()' to your input data prior to calling 'recombine()'")
    data <- addTransform(data, apply)
  }

  if(verbose)
    message("* Verifying suitability of 'output' for specified 'combine'...")

  if(is.character(output)) {
    class(output) <- c("character", paste0(tail(class(data), 1), "Char"))
    output <- charToOutput(output)
  }

  outClass <- ifelse(is.null(output), "nullConn", class(output)[1])
  if(!is.null(combine$validateOutput))
    if(!outClass %in% combine$validateOutput)
      stop("'output' of type ", outClass, " is not compatible with specified 'combine'")

  if(verbose)
    message("* Applying recombination...")

  map <- expression({
    for(i in seq_along(map.keys)) {
      if(combine$group) {
        key <- "1"
      } else {
        key <- map.keys[[i]]
      }
      if(is.function(combine$mapHook)) {
        tmp <- combine$mapHook(map.keys[[i]], map.values[[i]])
        if(is.null(tmp)) {
          map.values[i] <- list(NULL)
        } else {
          map.values[[i]] <- tmp
        }
      }
      collect(key, map.values[[i]])
    }
  })

  reduce <- combine$reduce

  parList <- list(combine = combine)
  # final is only used at the end
  # and its namespace conflicts in RHIPE
  parList$combine$final <- NULL

  for(ii in seq_along(parList$combine)) {
    if(is.function(parList$combine[[ii]]))
      environment(parList$combine[[ii]]) <- baseenv()
  }

  packages <- c(packages, "datadr")

  globalVarList <- drGetGlobals(apply)
  if(length(globalVarList$vars) > 0)
    parList <- c(parList, globalVarList$vars)

  # if the user supplies output as an unevaluated connection
  # the verbosity can be misleading
  suppressMessages(output <- output)

  res <- mrExec(data,
    map = map,
    reduce = reduce,
    output = output,
    overwrite = overwrite,
    params = c(parList, params),
    packages = c(globalVarList$packages, packages),
    control = control
  )

  if(is.null(output)) {
    return(combine$final(res))
  } else {
    return(res)
  }
}


