#' Recombine
#'
#' Apply an analytic recombination method to a ddo/ddf object and combine the results
#'
#' @param data an object of class "ddo" of "ddf"
#' @param apply a function specifying the analytic method to apply to each subset, or a pre-defined apply function (see \code{\link{drBLB}}, \code{\link{drGLM}}, for example).
#' NOTE: This argument is now deprecated in favor of \code{\link{addTransform}}
#' @param combine the method to combine the results.
#' See, for example, \code{\link{combCollect}}, \code{\link{combDdf}}, \code{\link{combDdo}}, \code{\link{combRbind}}, etc.  If \code{combine = NULL}, \code{\link{combCollect}} will be used if \code{output = NULL} and \code{\link{combDdo}} is used if \code{output} is specified.
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param verbose logical - print messages about what is being done
#'
#' @return Depends on \code{combine}: this could be a distributed data object, a data frame, a key-value list, etc.  See examples.
#'
#' @references
#' \itemize{
#'  \item \url{http://tessera.io}
#'  \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#'
#' @author Ryan Hafen
#' @seealso \code{\link{divide}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{drGLM}}, \code{\link{drBLB}}, \code{\link{combMeanCoef}}, \code{\link{combMean}}, \code{\link{combCollect}}, \code{\link{combRbind}}, \code{\link{drLapply}}
#'
#' @examples
#' ## in-memory example
#' ##---------------------------------------------------------
#'
#' # begin with an in-memory ddf (backed by kvMemory)
#' bySpecies <- divide(iris, by = "Species")
#'
#' # create a function to calculate the mean for each variable
#' colMean <- function(x) data.frame(lapply(x, mean))
#'
#' # apply the transformation
#' bySpeciesTransformed <- addTransform(bySpecies, colMean)
#'
#' # recombination with no 'combine' argument and no argument to output
#' # produces the key-value list produced by 'combCollect()'
#' recombine(bySpeciesTransformed)
#'
#' # but we can also preserve the distributed data frame, like this:
#' recombine(bySpeciesTransformed, combine = combDdf)
#'
#' # or we can recombine using 'combRbind()' and produce a data frame:
#' recombine(bySpeciesTransformed, combine = combRbind)
#'
#' ## local disk connection example with parallelization
#' ##---------------------------------------------------------
#'
#' # create a 2-node cluster that can be used to process in parallel
#' cl <- parallel::makeCluster(2)
#'
#' # create the control object we'll pass into local disk datadr operations
#' control <- localDiskControl(cluster = cl)
#' # note that setting options(defaultLocalDiskControl = control)
#' # will cause this to be used by default in all local disk operations
#'
#' # create local disk connection to hold bySpecies data
#' ldPath <- file.path(tempdir(), "by_species")
#' ldConn <- localDiskConn(ldPath, autoYes = TRUE)
#'
#' # convert in-memory bySpecies to local-disk ddf
#' bySpeciesLD <- convert(bySpecies, ldConn)
#'
#' # apply the transformation
#' bySpeciesTransformed <- addTransform(bySpeciesLD, colMean)
#'
#' # recombine the data using the transformation
#' bySpeciesMean <- recombine(bySpeciesTransformed,
#'   combine = combRbind, control = control)
#' bySpeciesMean
#'
#' # remove temporary directories
#' unlink(ldPath, recursive = TRUE)
#'
#' # shut down the cluster
#' parallel::stopCluster(cl)
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

  # if(verbose)
  #   message("* Verifying suitability of 'output' for specified 'combine'...")

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


