#' Sample Quantiles for 'ddf' Objects
#'
#' Compute sample quantiles for 'ddf' objects
#'
#' @param x a 'ddf' object
#' @param var the name of the variable to compute quantiles for
#' @param by an optional variable name or vector of variable names by which to group quantile computations
#' @param probs numeric vector of probabilities with values in [0-1]
#' @param preTransFn a transformation function (if desired) to applied to each subset prior to computing quantiles (here it may be useful for adding a "by" variable that is not present) - note: this transformation should not modify \code{var} (use \code{varTransFn} for that) - also note: this is deprecated - instead use \code{\link{addTransform}} prior to calling divide
#' @param varTransFn transformation to apply to variable prior to computing quantiles
#' @param varRange range of x (can be left blank if summaries have been computed)
#' @param nBins how many bins should the range of the variable be split into?
#' @param tails how many exact values at each tail should be retained?
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param \ldots additional arguments
#'
#' @return
#' data frame of quantiles \code{q} and their associated f-value \code{fval}.  If \code{by} is specified, then also a variable \code{group}.
#'
#' @details
#' This division-agnostic quantile calculation algorithm takes the range of the variable of interest and splits it into \code{nBins} bins, tabulates counts for those bins, and reconstructs a quantile approximation from them.  \code{nBins} should not get too large, but larger \code{nBins} gives more accuracy.  If \code{tails} is positive, the first and last \code{tails} ordered values are attached to the quantile estimate - this is useful for long-tailed distributions or distributions with outliers for which you would like more detail in the tails.
#'
#' @author Ryan Hafen
#' @seealso \code{\link{updateAttributes}}
#'
#' @examples
#' # break the iris data into k/v pairs
#' irisSplit <- list(
#'   list("1", iris[1:10,]), list("2", iris[11:110,]), list("3", iris[111:150,])
#' )
#' # represent it as ddf
#' irisSplit <- ddf(irisSplit, update = TRUE)
#'
#' # approximate quantiles over the divided data set
#' probs <- seq(0, 1, 0.005)
#' iq <- drQuantile(irisSplit, var = "Sepal.Length", tails = 0, probs = probs)
#' plot(iq$fval, iq$q)
#'
#' # compare to the all-data quantile "type 1" result
#' plot(probs, quantile(iris$Sepal.Length, probs = probs, type = 1))
#'
#' @export
drQuantile <- function(x, var, by = NULL, probs = seq(0, 1, 0.005), preTransFn = NULL, varTransFn = identity, varRange = NULL, nBins = 10000, tails = 100, params = NULL, packages = NULL, control = NULL, ...) {
  # nBins <- 10000; tails <- 0; probs <- seq(0, 1, 0.0005); by <- "Species"; var <- "Sepal.Length"; x <- ldd; trans <- identity

  # we need to know the range of the variables, which we don't
  # when we have a transformed object - and can't update attributes
  # (transformed objects are meant to be intermediate objects anyway)
  if(inherits(x, "transformed") && is.null(varRange)) {
    stop("Cannot run drQuantile() on a transformed divided data object without varRange being specified explicitly.  Please specify varRange or first call drPersist() on this data to make transformation persistent.", call. = FALSE)
  }

  if(!inherits(x, "ddf")) {
    stop("Need a distributed data frame.")
  }

  if(class(attr(x, "ddf")$summary)[1] == "logical")
    stop("Need to know the range of the variable to compute quantiles - please run updateAttributes on this data.")

  if(!is.null(preTransFn)) {
    message("** note **: preTransFn is deprecated - please apply this transformation using 'addTransform()' to your input data prior to calling 'drQuantile()'")
    x <- addTransform(x, preTransFn)
  }
  ex <- kvExample(x)

  if(is.null(varRange)) {
    rng <- varTransFn(attr(x, "ddf")$summary[[var]]$range)
  } else {
    rng <- varRange
  }
  delta <- diff(rng) / (nBins - 1)
  cuts <- seq(rng[1] - delta / 2, rng[2] + delta / 2, by = delta)
  mids <- seq(rng[1], rng[2], by = delta)

  map <- expression({
    dat <- data.frame(data.table::rbindlist(lapply(seq_along(map.values), function(i) {
      res <- data.frame(
        v = varTransFn(map.values[[i]][, var]),
        map.values[[i]][, by, drop = FALSE],
        stringsAsFactors = FALSE
      )
      res
    })))

    if(is.null(by)) {
      inds <- list("1" = seq_len(nrow(dat)))
    } else {
      splits <- getCondCuts(dat[, by, drop = FALSE], by)
      inds <- split(seq_along(splits), splits)
    }
    indsNms <- names(inds)
    for(ii in seq_along(inds)) {
      vv <- dat$v[inds[[ii]]]
      vv <- vv[!is.na(vv)]
      if(length(vv) > 0) {
        ord <- order(vv)

        cutTab <- as.data.frame(table(cut(vv, cuts, labels = FALSE)), responseName = "Freq", stringsAsFactors = FALSE)
        cutTab$Var1 <- as.integer(cutTab$Var1)

        if(nrow(cutTab) == 0) {
          warning("data outside specified range was found in quantile calculation - please make sure the correct range limits are being used")
        } else {
          for(i in 1:nrow(cutTab)) {
            collect(list(as.list(dat[inds[[ii]][1], by, drop = FALSE]), cutTab$Var1[i]), cutTab$Freq[i])
          }
          collect(list(as.list(dat[inds[[ii]][1], by, drop = FALSE]), "bot"), vv[head(ord, tails)])
          collect(list(as.list(dat[inds[[ii]][1], by, drop = FALSE]), "top"), vv[tail(ord, tails)])
        }
      }
    }
  })

  reduce <- expression(
    pre = {
      bot <- NULL
      top <- NULL
      sum <- 0
    }, reduce = {
      # if(length(reduce.key[[2]]) == 0)
      #   browser()
      if(reduce.key[[2]] == "bot") {
        bot <- head(sort(c(bot, do.call(c, reduce.values))), tails)
      } else if(reduce.key[[2]] == "top") {
        top <- tail(sort(c(top, do.call(c, reduce.values))), tails)
      } else {
        sum <- sum + sum(unlist(reduce.values))
      }
    }, post = {
      # if(length(reduce.key[[2]]) == 0)
      #   browser()
      if(reduce.key[[2]] == "bot") {
        collect(reduce.key, bot)
      } else if(reduce.key[[2]] == "top") {
        collect(reduce.key, top)
      } else {
        collect(reduce.key, sum)
      }
    }
  )

  globalVarList <- drGetGlobals(varTransFn)
  parList <- list(
    varTransFn = varTransFn,
    var = var,
    by = by,
    cuts = cuts,
    tails = tails
  )

  packages <- c(packages, "datadr", "data.table")

  mrRes <- mrExec(x,
    map = map,
    reduce = reduce,
    params = c(globalVarList$vars, parList, params),
    packages = c(globalVarList$packages, packages),
    control = control
  )

  # put things together...
  mrRes <- getAttribute(mrRes, "conn")$data

  if(is.null(by)) {
    res <- constructQuants(mrRes, probs, tails, mids)
  } else {
    groups <- sapply(mrRes, function(x) {
      do.call(paste, c(as.list(x[[1]][[1]]), sep = "|"))
    })
    ind <- split(seq_along(groups), groups)

    res <- lapply(seq_along(ind), function(i) {
      data.frame(
        constructQuants(mrRes[ind[[i]]], probs, tails, mids),
        mrRes[[ind[[i]][1]]][[1]][[1]],
        stringsAsFactors = FALSE
      )
    })
    res <- data.frame(data.table::rbindlist(res))
  }

  res
}

constructQuants <- function(obj, probs, tails, mids) {
  keys <- lapply(obj, function(x) x[[1]][[2]])
  intKeys <- sapply(keys, is.integer)
  vals <- lapply(obj, "[[", 2)

  quants <- data.frame(
    idx = unlist(keys[intKeys]),
    freq = unlist(vals[intKeys])
  )

  tot <- sum(as.numeric(quants$freq))
  quants <- quants[order(quants$idx),]
  quants$pct <- quants$freq / tot
  quants$cpct <- cumsum(quants$pct)
  quants$q <- mids[quants$idx]

  fn <- approxfun(quants$cpct, quants$q, method = "constant", f = 1, rule = 2)
  res <- data.frame(
    fval = probs,
    q = fn(probs)
  )

  if(tails > 0) {
    # now append top and bottom
    tailKeys <- unlist(keys[!intKeys])
    tailVals <- vals[!intKeys]

    top <- tailVals[tailKeys == "top"][[1]]
    bot <- tailVals[tailKeys == "bot"][[1]]

    botDf <- data.frame(
      fval = (seq_len(tails) - 1) / tot,
      q = bot
    )

    topDf <- data.frame(
      fval = (seq_len(tails) + tot - tails) / tot,
      q = top
    )

    res <- res[res$fval > max(botDf$fval) & res$fval < min(topDf$fval),]
    res <- rbind(botDf, res, topDf)
  }
  res
}
