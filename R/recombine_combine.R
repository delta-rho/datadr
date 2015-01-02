#' Mean Coefficient Recombination
#'
#' Mean coefficient recombination
#'
#' @param \ldots ...
#'
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.  It expects to be dealing with named vectors including an element \code{n} specifying the number of rows in that subset.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}, \code{\link{combCollect}}, \code{\link{combDdo}}, \code{\link{combRbind}}, \code{\link{combMean}}
#'
#' @export
combMeanCoef <- function(...) {
  structure(
  list(
    reduce = expression(
      pre = {
        res <- list()
        n <- as.numeric(0)
        coefNames <- NULL
      },
      reduce = {
        if(is.null(coefNames))
          coefNames <- reduce.values[[1]]$names

        n <- sum(c(n, unlist(lapply(reduce.values, function(x) x$n))), na.rm = TRUE)
        res <- do.call(rbind, c(res, lapply(reduce.values, function(x) {
          x$coef * x$n
        })))
        res <- apply(res, 2, sum)
      },
      post = {
        res <- res / n
        names(res) <- coefNames
        collect("final", res)
      }
    ),
    final = function(x, ...) x[[1]][[2]],
    validateOutput = c("nullConn"),
    group = TRUE,
    ...
  ),
  class = "combMeanCoef")
}

#' Mean Recombination
#'
#' Mean recombination
#'
#' @param \ldots ...
#'
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combCollect}}, \code{\link{combDdo}}, \code{\link{combRbind}}, \code{\link{combMeanCoef}}
#'
#' @export
combMean <- function(...) {
  structure(
  list(
    reduce = expression(
      pre = {
        suppressWarnings(suppressMessages(require(data.table)))
        res <- list()
        n <- as.numeric(0)
      },
      reduce = {
        n <- sum(c(n, length(reduce.values)))
        res <- do.call(rbind, c(res, lapply(reduce.values, function(x) {
          x
        })))
        res <- apply(res, 2, sum)
      },
      post = {
        res <- res / n
        collect("final", res)
      }
    ),
    final = function(x, ...) {
      if(length(x) == 1) {
        return(x[[1]][[2]])
      } else {
        return(getAttribute(x, "conn")$data)
      }
    } ,
    validateOutput = c("nullConn"),
    group = TRUE,
    ...
  ),
  class = "combMean")
}

#' "DDO" Recombination
#'
#' "DDO" recombination - simply collect the results into a "ddo" object
#'
#' @param \ldots ...
#'
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combCollect}}, \code{\link{combMeanCoef}}, \code{\link{combRbind}}, \code{\link{combMean}}
#'
#' @export
combDdo <- function(...) {
  structure(
  list(
    reduce = expression(reduce = {
      lapply(reduce.values, function(r) collect(reduce.key, r))
    }),
    final = identity,
    validateOutput = c("localDiskConn", "hdfsConn", "nullConn"),
    group = FALSE,
    ...
  ),
  class = "combCollect")
}

#' "Collect" Recombination
#'
#' "Collect" recombination - simply collect the results into a local list of key-value pairs
#'
#' @param \ldots ...
#'
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combDdo}}, \code{\link{combMeanCoef}}, \code{\link{combRbind}}, \code{\link{combMean}}
#'
#' @export
combCollect <- function(...) {
  structure(
  list(
    reduce = expression(reduce = {
      lapply(reduce.values, function(r) collect(reduce.key, r))
    }),
    final = function(x, ...)
      lapply(getAttribute(x, "conn")$data, function(y) {
        class(y) <- "kvPair"
        names(y) <- c("key", "value")
        y
      }),
    validateOutput = c("nullConn"),
    group = FALSE,
    ...
  ),
  class = "combCollect")
}

#' "rbind" Recombination
#'
#' "rbind" recombination
#'
#' @param \ldots ...
#'
#' @details This is an experimental prototype.  It is to be passed as the argument \code{combine} to \code{\link{recombine}}.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combDdo}}, \code{\link{combCollect}}, \code{\link{combMeanCoef}}, \code{\link{combMean}}
#'
#' @export
combRbind <- function(...) {
  red <- expression(
    pre = {
      adata <- list()
    },
    reduce = {
      adata[[length(adata) + 1]] <- reduce.values
    },
    post = {
      adata <- do.call(rbind, unlist(adata, recursive = FALSE))
      collect(reduce.key, adata)
    }
  )
  # attr(red, "combine") <- TRUE

  structure(
  list(
    reduce = red,
    final = function(x, ...) {
      if(length(x) == 1) {
        return(x[[1]][[2]])
      } else {
        return(getAttribute(x, "conn")$data)
      }
    },
    mapHook = function(key, value) {
      attrs <- attributes(value)
      if(!is.null(attrs$split)) {
        if(!is.data.frame(value)) {
          value <- list(val = value)
        }
        value <- data.frame(c(attrs$split, as.list(value)), stringsAsFactors = FALSE)
      }
      value
    },
    validateOutput = c("nullConn"),
    group = TRUE,
    ...
    # TODO: should make sure the result won't be too big (approximate by size of output from test run times number of divisions)
  ),
  class = "combRbind")
}
