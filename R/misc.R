#' Apply Function to Key-Value Pair
#'
#' Apply a function to a single key-value pair - not a traditional R "apply" function.
#'
#' @param kvPair a key-value pair (a list with 2 elements or object created with \code{\link{kvPair}})
#' @param fn a function
#'
#' @details Determines how a function should be applied to a key-value pair and then applies it: if the function has two formals, it applies the function giving it the key and the value as the arguments; if the function has one formal, it applies the function giving it just the value.  The function is assumed to return a value unless the result is a \code{\link{kvPair}} object.  When the function returns a value the original key will be returned in the resulting key-value pair.
#'
#' This provides flexibility and simplicity for when a function is only meant to be applied to the value (the most common case), but still allows keys to be used if desired.
#'
#' @examples
#' kv <- kvPair(1, 2)
#' kv
#' kvApply(kv, function(x) x^2)
#' kvApply(kv, function(k, v) v^2)
#' kvApply(kv, function(k, v) k + v)
#' kvApply(kv, function(x) kvPair("new_key", x))
#' @export
kvApply <- function(kvPair, fn) {
  # TODO?: also do other stuff like add splitVars back on to df, etc. prior to applying
  if(length(formals(fn)) == 2) {
    res <- fn(kvPair[[1]], kvPair[[2]])
  } else {
    res <- fn(kvPair[[2]])
  }
  if(!inherits(res, "kvPair"))
    res <- kvPair(kvPair[[1]], res)
  return(res)
}

#' Convert dplyr grouped_df to ddf
#'
#' @param x a grouped_df object from dplyr
#' @examples
#' \dontrun{
#' library(dplyr)
#' bySpecies <- iris %>%
#'   group_by(Species) %>%
#'   to_ddf()
#' }
#' @export
#' @importFrom dplyr do
to_ddf <- function(x) {
  if(!inherits(x, "grouped_df"))
    stop("to_ddf currently only operates on grouped dplyr tbls")

  tmp <- do(x, kv = list("key", data.frame(.)))
  # tmp <- do_(x, kv = ~.)

  n <- length(tmp)
  keys <- data.frame(lapply(tmp[1:(n-1)], as.character), stringsAsFactors = FALSE)
  keynms <- matrix(apply(keys, 1, function(x) paste(colnames(keys), x, sep = "=")), nrow = n - 1)
  keynms <- apply(keynms, 2, function(x) paste(x, collapse = "|"))
  for(ii in seq_along(tmp$kv)) {
    tmp$kv[[ii]][[1]] <- keynms[ii]
    attr(tmp$kv[[ii]][[2]], "split") <- keys[ii,,drop = FALSE]
  }

  ddf(tmp$kv)
}

#' Pipe data
#'
#' @importFrom magrittr %>%
#' @name %>%
#' @rdname pipe
#' @export
#' @param lhs a data object
#' @param rhs a function to apply to the data
NULL




### internal

# maybe hash functions should be exposed?
keyHash <- function(key, nBins) {
  sum(strtoi(charToRaw(digest(key)), 16L)) %% nBins
}

digestKeyHash <- function(digestKey, nBins) {
  sum(strtoi(charToRaw(digestKey), 16L)) %% nBins
}

validateListKV <- function(data) {
  # make sure it is a valid k/v list
  allLengthTwo <- !any(sapply(data, length) != 2)
  allLists <- !any(!sapply(data, is.list))
  if(!allLengthTwo || !allLists)
    stop("List must be a list of lists, each sublist being of length two (k/v pairs)")

  # # add "keyValue" class to each element
  # for(i in seq_along(data)) {
  #   class(data[[i]]) <- c("keyValue", "list")
  # }
}

nullAttributes <- function (e) {
  environment(e) <- NULL

  for (i in seq_along(e)) attributes(e[[i]]) <- NULL
  eval(parse(text = deparse(e)))
}

appendExpression <- function(expr1, expr2) {
  getLines <- function(expr) {
    if(inherits(expr, "expression")) {
      expr <- deparse(expr)
      n <- length(expr)
      if(n == 1) {
        warning("expression must be wrapped in {}: ", expr)
        return(expr)
      } else {
        return(expr[-c(1, n)])
      }
    } else {
      return("")
    }
  }

  eval(parse(text = paste("expression({", paste(c(getLines(expr1), getLines(expr2)), collapse = "\n"), "})", sep = "")))
}

isAbsolutePath <- function(path) {
  splitPath <- strsplit(path, split = "[/\\]")[[1]]

  any(
    grepl("^~", path),
    grepl("^.:(/|\\\\)", path),
    splitPath[1] == ""
  )
}

