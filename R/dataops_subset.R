#' Subsetting Distributed Data Frames
#'
#' Return a subset of a "ddf" object to memory
#'
#' @param data object to be subsetted -- an object of class "ddf" or "ddo" - in the latter case, need to specify \code{preTransFn} to coerce each subset into a data frame
#' @param subset logical expression indicating elements or rows to keep: missing values are taken as false
#' @param select expression, indicating columns to select from a data frame
#' @param drop passed on to [ indexing operator
#' @param preTransFn a transformation function (if desired) to applied to each subset prior to division - note: this is deprecated - instead use \code{\link{addTransform}} prior to calling divide
#' @param maxRows the maximum number of rows to return
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param verbose logical - print messages about what is being done
#'
#' @return data frame
#'
#' @author Ryan Hafen
#' @examples
#' d <- divide(iris, by = "Species")
#' drSubset(d, Sepal.Length < 5)
#' @export
drSubset <- function(data,
  subset = NULL,
  select = NULL,
  drop = FALSE,
  preTransFn = NULL,
  maxRows = 500000,
  params = NULL,
  packages = NULL,
  control = NULL,
  verbose = TRUE
) {

  if(!inherits(data, "ddf")) {
    if(verbose)
      message("* Input data is not 'ddf' - attempting to cast it as such")
    data <- ddf(data)
  }

  if(!is.null(preTransFn)) {
    message("** note **: preTransFn is deprecated - please apply this transformation using 'addTransform()' to your input data prior to calling 'drSubset()'")
    data <- addTransform(data, preTransFn)
  }

  # get an example of what a subset will look like
  ex <- kvExample(data)[[2]]

  if(verbose)
    message("* Testing 'subset' on a subset")

  if(is.null(select))
    select <- TRUE

  if(missing(subset)) {
    subset <- NULL
  } else {
    subset <- substitute(subset)
  }

  r <- if(is.null(subset)) {
	  rep_len(TRUE, nrow(ex))
  } else {
	  r <- eval(subset, ex, parent.frame())
      if(!is.logical(r)) stop("'subset' must be logical")
	  r & !is.na(r)
  }
  test <- ex[r, select, drop = drop]

  parList <- list(
    maxRows = maxRows,
    subset = subset,
    select = select,
    drop = drop
  )

  packages <- unique(c(packages, "datadr", "data.table"))

  map <- expression({
    df <- data.frame(data.table::rbindlist(map.values))

    r <- if(is.null(subset)) {
  	  rep_len(TRUE, nrow(df))
    } else {
  	  r <- eval(subset, df, parent.frame())
        if(!is.logical(r)) stop("'subset' must be logical")
  	  r & !is.na(r)
    }
    res <- df[r, select, drop = drop]

    counter("datadr", "totalFilteredRows", nrow(res))
    collect("1", res)
  })

  reduce <- expression(
    pre = {
      df <- list()
      nRows <- 0
    },
    reduce = {
      nRows <- nRows + sapply(reduce.values, nrow)
      if(nRows < maxRows)
        df[[length(df) + 1]] <- reduce.values
    },
    post = {
      df <- data.frame(data.table::rbindlist(unlist(df, recursive = FALSE)))
      df <- df[1:min(nrow(df), maxRows),]
      collect(reduce.key, df)
    }
  )

  res <- mrExec(data,
    map     = map,
    reduce   = reduce,
    params   = c(params, parList),
    packages  = packages,
    control  = control
  )

  # TODO: checking on whether counter is larger than
  # number of rows of result

  res[[1]][[2]]
}


