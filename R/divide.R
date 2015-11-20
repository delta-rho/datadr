#' Divide a Distributed Data Object
#'
#' Divide a ddo/ddf object into subsets based on different criteria
#'
#' @param data an object of class "ddf" or "ddo" - in the latter case, need to specify \code{preTransFn} to coerce each subset into a data frame
#' @param by specification of how to divide the data - conditional (factor-level or shingles), random replicate, or near-exact replicate (to come) -- see details
#' @param bsvFn a function to be applied to each subset that returns a list of between subset variables (BSVs)
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param overwrite logical; should existing output location be overwritten? (also can specify \code{overwrite = "backup"} to move the existing output to _bak)
#' @param spill integer telling the division method how many lines of data should be collected until spilling over into a new key-value pair
#' @param filterFn a function that is applied to each candidate output key-value pair to determine whether it should be (if returns \code{TRUE}) part of the resulting division
#' @param preTransFn a transformation function (if desired) to applied to each subset prior to division - note: this is deprecated - instead use \code{\link{addTransform}} prior to calling divide
#' @param postTransFn a transformation function (if desired) to apply to each post-division subset
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param update should a MapReduce job be run to obtain additional attributes for the result data prior to returning?
#' @param verbose logical - print messages about what is being done
#'
#' @return an object of class "ddf" if the resulting subsets are data frames.  Otherwise, an object of class "ddo".
#'
#' @details The division methods this function will support include conditioning variable division for factors (implemented -- see \code{\link{condDiv}}), conditioning variable division for numerical variables through shingles, random replicate (implemented -- see \code{\link{rrDiv}}), and near-exact replicate.  If \code{by} is a vector of variable names, the data will be divided by these variables.  Alternatively, this can be specified by e.g.  \code{condDiv(c("var1", "var2"))}.
#'
#' @references
#' \itemize{
#'  \item \url{http://www.datadr.org}
#'  \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#'
#' @author Ryan Hafen
#' @seealso \code{\link{recombine}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{condDiv}}, \code{\link{rrDiv}}
#' @export
divide <- function(data,
  by = NULL,
  spill = 1000000,
  filterFn = NULL,
  bsvFn = NULL,
  output = NULL,
  overwrite = FALSE,
  preTransFn = NULL,
  postTransFn = NULL,
  params = NULL,
  packages = NULL,
  control = NULL,
  update = FALSE,
  verbose = TRUE
) {
  # data <- ddf(localDiskConn("~/Desktop/asdf")); by <- "Species"; bsvFn <- NULL; preTransFn <- NULL; postTransFn <- NULL; output <- localDiskConn("~/Desktop/asdf3"); control <- NULL; update <- FALSE

  if(!is.list(by)) { # by should be a list
    by <- condDiv(by)
  }

  if(by$type ==  "condDiv" && is.data.frame(data)) {
    # preTransFn is ignored
    res <- getDivideDF(data, by = by, postTransFn = postTransFn,
      bsvFn = bsvFn, update = update)

    suppressMessages(output <- output)
    return(convert(res, output, overwrite = overwrite))
  }

  if(!inherits(data, "ddf")) {
    if(verbose)
      message("* Input data is not 'ddf' - attempting to cast it as such")
    data <- ddf(data)
  }

  if(verbose)
    message("* Verifying parameters...")

  seed <- NULL
  if(by$type == "rrDiv")
    seed <- by$seed

  if(is.null(seed))
    seed <- as.integer(runif(1)*1000000)

  if(!is.null(preTransFn)) {
    message("** note **: preTransFn is deprecated - please apply this transformation using 'addTransform()' to your input data prior to calling 'divide()'")
    data <- addTransform(data, preTransFn)
  }

  if(is.null(filterFn)) {
    filterFn <- function(x) TRUE
    environment(filterFn) <- .GlobalEnv
  }

  # get an example of what a subset will look like when it is to be divided
  ex <- kvExample(data)

  # make sure the division specification is good
  # and get any parameters that need to be sent to dfSplit
  by$specVars <- validateDivSpec(by, data, ex)

  if(!is.null(bsvFn)) {
    # Even though bsvFn is called on subsets *after* division
    # it is good to validate here since an error after would
    # be a waste of a lot of computation only to abort.
    # Since bsvFn is applied *before* postTransFn() is called
    # it should work on an input subset after preTransFn().

    validateBsvFn(ex, bsvFn, verbose = TRUE)
  }
  if(verbose)
    message("* Applying division...")

  parList <- list(
    by = by,
    transFn = getAttribute(data, "transFn"),
    postTransFn = postTransFn,
    seed = seed,
    bsvFn = bsvFn,
    MAX_ROWS = spill,
    filterFn = filterFn
  )

  # find globals in postTransFn, bsvFn, and filterFn
  postGlobals <- drGetGlobals(postTransFn)
  bsvGlobals <- drGetGlobals(bsvFn)
  filterGlobals <- drGetGlobals(filterFn)

  globalVarList <- c(postGlobals$vars, bsvGlobals$vars, filterGlobals$vars)
  packages <- unique(c(postGlobals$packages, bsvGlobals$packages, filterGlobals$packages, packages))

  if(length(globalVarList) > 0)
    parList <- c(parList, globalVarList$vars)

  packages <- c(packages, "datadr")

  setup <- as.expression(bquote({
    seed <- .(seed)
    # setupRNGStream(seed)
  }))

  map <- expression({
    for(i in seq_along(map.values)) {
      if(length(map.values[[i]]) > 0) {
        cutDat <- dfSplit(map.values[[i]], by, seed)
        cdn <- names(cutDat)

        for(j in seq_along(cutDat)) {
          collect(cdn[j], cutDat[[j]])
        }
      }

      # counter("datadr", "Divide map k/v processed", 1)
      # counter("datadr", "Divide map k/v emitted", length(cutDat))
      # counter("datadr", "Divide map pre-division bytes", as.integer(object.size(r)))
      # counter("datadr", "Divide map post-division bytes", sum(sapply(cutDat, function(x) as.integer(object.size(x)))))
    }
  })

  reduce <- expression(
    pre = {
      # df, nr, and count used for "spill"
      df <- list()
      nr <- 0
      spillCount <- 0

      # counter("datadr", "Divide reduce k/v processed", 1)
    },
    reduce = {
      df[[length(df) + 1]] <- reduce.values
      nr <- nr + sum(sapply(reduce.values, nrow))
      if(nr > MAX_ROWS) {
        # this df is ready to go

        for(ii in seq_len(floor(nr / MAX_ROWS))) {
          spillCount <- spillCount + 1
          df <- data.frame(data.table::rbindlist(unlist(df, recursive = FALSE)))

          # do what is needed and collect
          if(kvApply(list(reduce.key, df[1:MAX_ROWS,]), filterFn)$value) {
            # counter("datadr", "spilled", 1)
            # put in div-specific attr stuff
            collect(paste(reduce.key, spillCount, sep = "_"),
              addSplitAttrs(df[1:MAX_ROWS,], bsvFn, by, postTransFn))
          }
          # now continue to work on what is left over
          df <- list(list(df[c((MAX_ROWS + 1):nr),]))
          nr <- nr - MAX_ROWS
        }
      }
    },
    post = {
      df <- data.frame(data.table::rbindlist(unlist(df, recursive = FALSE)))
      # if we never reached MAX_ROWS, don't append count to key
      if(spillCount > 0)
        reduce.key <- paste(reduce.key, spillCount + 1, sep = "_")

      if(kvApply(list(reduce.key, df), filterFn)$value) {
        # put in div-specific attr stuff
        collect(reduce.key, addSplitAttrs(df, bsvFn, by, postTransFn))
      }
    }
  )

  # if the user supplies output as an unevaluated connection
  # the verbosity can be misleading
  suppressMessages(output <- output)

  res <- mrExec(data,
    setup    = setup,
    map     = map,
    reduce   = reduce,
    output   = output,
    overwrite = overwrite,
    control  = control,
    params   = c(params, parList),
    packages  = packages
  )

  if(update)
    res <- updateAttributes(res)

  # add an attribute specifying how it was divided
  res <- setAttributes(res, list(div = list(divBy = by)))

  # add bsv attributes
  if(!is.null(bsvFn)) {
    desc <- getBsvDesc(kvExample(data)[[2]], bsvFn)
    tmp <- list(bsvFn = bsvFn, bsvDesc = desc)
    class(tmp) <- c("bsvInfo", "list")
    res <- setAttributes(res, list(bsvInfo = tmp))
  }

  res
}

#' Get Between Subset Variable
#'
#' For a given key-value pair, get a BSV variable value by name (if present)
#' @param x a key-value pair or a value
#' @param name the name of the BSV to get
#' @export
getBsv <- function(x, name) {
  res <- attr(x, "bsv")[[name]]
  if(is.null(res))
    res <- attr(x[[2]], "bsv")[[name]]
  res
}

#' Get Between Subset Variables
#'
#' For a given key-value pair, exract all BSVs
#' @param x a key-value pair or a value
#' @export
getBsvs <- function(x) {
  res <- attr(x, "bsv")
  if(is.null(res))
    res <- attr(x[[2]], "bsv")
  res
}

#' Extract "Split" Variable
#'
#' For a given key-value pair or value, get a split variable value by name, if present (split variables are variables that define how the data was divided).
#' @param x a key-value pair or a value
#' @param name the name of the split variable to get
#' @export
getSplitVar <- function(x, name) {
  res <- attr(x, "split")[[name]]
  if(is.null(res))
    res <- attr(x[[2]], "split")[[name]]
  res
}

#' Extract "Split" Variables
#'
#' For a given k/v pair or value, exract all split variables (split variables are variables that define how the data was divided).
#' @param x a key-value pair or a value
#' @export
getSplitVars <- function(x) {
  res <- as.list(attr(x, "split"))
  if(length(res) == 0)
    res <- as.list(attr(x[[2]], "split"))
  if(length(res) == 0)
    res <- NULL
  res
}

#' "Flatten" a ddf Subset
#'
#' Add split variables and BSVs (if any) as columns to a subset of a ddf.
#' @param x a value of a key-value pair
#' @seealso \code{\link{getSplitVars}}, \code{\link{getBsvs}}
#' @export
flatten <- function(x) {
  svs <- getSplitVars(x)
  bsvs <- getBsvs(x)
  data.frame(c(x, svs, bsvs))
}

# take a data frame (or one that becomes a data frame with preTransFn)
# and split it according to "by" and return a named list
# (this is meant to be called in parallel)

#' Functions used in divide()
#' @name divide-internals
#' @rdname divide-internals
#' @param curDF,seed arguments
#' @export
#' @note These functions can be ignored.  They are only exported to make their use in a distributed setting more convenient.
dfSplit <- function(curDF, by, seed) {
  # remove factor levels, if any
  # TODO: keep track of factor levels
  factorInd <- which(sapply(curDF, is.factor))
  for(i in seq_along(factorInd)) {
    curDF[[factorInd[i]]] <- as.character(curDF[[factorInd[i]]])
  }

  split(curDF, getCuts(by, curDF))
}

#' @rdname divide-internals
#' @param curSplit,bsvFn,by,postTransFn arguments
#' @export
addSplitAttrs <- function(curSplit, bsvFn, by, postTransFn = NULL) {
  bsvs <- NULL

  # BSVs are applied before postTrans
  if(!is.null(bsvFn)) {
    bsvs <- stripBsvAttr(bsvFn(curSplit))
  }

  splitAttr <- NULL
  if(by$type == "condDiv") {
    splitVars <- by$vars
    splitAttr <- curSplit[1, splitVars, drop = FALSE]
  }

  if(!is.null(postTransFn)) {
    curSplit <- postTransFn(curSplit)
  } else {
    if(by$type == "condDiv") {
      # remove columns for split variables
      curSplit <- curSplit[,setdiff(names(curSplit), by$vars), drop = FALSE]
    }
  }

  attr(curSplit, "bsv") <- bsvs
  attr(curSplit, "split") <- splitAttr

  curSplit
}

