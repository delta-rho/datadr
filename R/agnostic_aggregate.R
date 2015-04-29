#' Division-Agnostic Aggregation
#'
#' Aggregates data by cross-classifying factors, with a formula interface similar to \code{xtabs}
#'
#' @param formula a \code{\link{formula}} object with the cross-classifying variables (separated by +) on the right hand side (or an object which can be coerced to a formula). Interactions are not allowed. On the left hand side, one may optionally give a variable name in the data representing counts; in the latter case, the columns are interpreted as corresponding to the levels of a variable. This is useful if the data have already been tabulated.
#' @param data a "ddf" containing the variables in the formula \code{formula}
#' @param by an optional variable name or vector of variable names by which to split up tabulations (i.e. tabulate independently inside of each unique "by" variable value).  The only difference between specifying "by" and placing the variable(s) in the right hand side of the formula is how the computation is done and how the result is returned.
#' @param output "kvConnection" object indicating where the output data should reside in the case of \code{by} being specified (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param preTransFn an optional function to apply to each subset prior to performing tabulation.  The output from this function should be a data frame containing variables with names that match that of the formula provided.  Note: this is deprecated - instead use \code{\link{addTransform}} prior to calling divide.
#' @param maxUnique the maximum number of unique combinations of variables to obtain tabulations for.  This is meant to help against cases where a variable in the formula has a very large number of levels, to the point that it is not meaningful to tabulate and is too computationally burdonsome.  If \code{NULL}, it is ignored.  If a positive number, only the top and bottom \code{maxUnique} tabulations by frequency are kept.
#' @param params a named list of parameters external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#'
#' @return a data frame of the tabulations.  When "by" is specified, it is a ddf with each key-value pair corresponding to a unique "by" value, containing a data frame of tabulations.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{xtabs}}, \code{\link{updateAttributes}}
#'
#' @note The interface is similar to \code{\link{xtabs}}, but instead of returning a full contingency table, data is returned in the form of a data frame only with rows for which there were positive counts.  This result is more similar to what is returned by \code{\link{aggregate}}.
#'
#' @examples
#' drAggregate(Sepal.Length ~ Species, data = ddf(iris))
#' @export
drAggregate <- function(formula, data = data, by = NULL, output = NULL, preTransFn = NULL, maxUnique = NULL, params = NULL, packages = NULL, control = NULL) {

  # TODO: check formula on a subset
  # if the number of levels of each factor is too insanely large
  # then warn or stop

  if(!is.null(preTransFn)) {
    message("** note **: preTransFn is deprecated - please apply this transformation using 'addTransform()' to your input data prior to calling 'drAggregate()'")
    data <- addTransform(data, preTransFn)
  }

  # if formula does not contain the "by" variable, add it in
  if(!is.null(by)) {
    for(i in seq_along(by)) {
      if(!by[i] %in% labels(terms(formula)))
        formula <- eval(parse(text = paste("update(formula, ~ . + ", by[i], ")", sep = "")))
    }
  }

  packages <- c(packages, "datadr")

  map <- expression({
    # tabulate all, group, and then collect for each unique "by"
    res <- do.call(rbind, lapply(seq_along(map.values), function(i) {
      tabulateMap(formula, map.values[[i]])
    }))

    if(length(res) > 0) {
      tmp <- xtabs(Freq ~ ., data = res)
      # tabulation of all NAs yields empty table
      if(length(tmp) > 0) {
        tmp <- as.data.frame(tmp, stringsAsFactors = FALSE)
        res <- subset(tmp, Freq > 0)
        if(is.null(by)) {
          collect("global", res)
        } else {
          # go through for each "by"
          splits <- getCondCuts(res[, by, drop = FALSE], by)
          inds <- split(seq_along(splits), splits)
          indsNms <- names(inds)
          lapply(seq_along(inds), function(ii) {
            if(length(inds[[ii]]) > 0)
              collect(indsNms[ii], res[inds[[ii]],])
          })
        }
      }
    }
  })

  reduce <- expression(
    pre = { res <- NULL },
    reduce = { res <- tabulateReduce(res, reduce.values, maxUnique) },
    post = { collect(reduce.key, res) }
  )

  parList <- list(
    formula = formula,
    by = by,
    maxUnique = maxUnique
  )

  # if the user supplies output as an unevaluated connection
  # the verbosity can be misleading
  suppressMessages(output <- output)

  res <- mrExec(data,
    map = map,
    reduce = reduce,
    output = output,
    params = c(parList, params),
    packages = packages,
    control = control
  )

  if(!is.null(by))
    return(res)

  res <- as.list(res)
  nms <- sapply(res, "[[", 1)
  res <- lapply(res, "[[", 2)
  names(res) <- nms
  if(length(res) == 1 && nms[1] == "global")
    res <- res[[1]]

  res
}
