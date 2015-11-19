#' HexBin Aggregation for Distributed Data Frames
#'
#' Create "hexbin" object of hexagonally binned data for a distributed data frame.  This computation is division agnostic - it does not matter how the data frame is split up.
#'
#' @param data a distributed data frame
#' @param xVar,yVar names of the variables to use
#' @param by an optional variable name or vector of variable names by which to group hexbin computations
#' @param xTransFn,yTransFn a transformation function to apply to the x and y variables prior to binning
#' @param xRange,yRange range of x and y variables (can be left blank if summaries have been computed)
#' @param xbins the number of bins partitioning the range of xbnds
#' @param shape the shape = yheight/xwidth of the plotting regions
#' @param params a named list of objects external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param packages a vector of R package names that contain functions used in \code{fn} (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#'
#' @return a "hexbin" object
#'
#' @references Carr, D. B. et al. (1987) Scatterplot Matrix Techniques for Large \eqn{N}. \emph{JASA} \bold{83}, 398, 424--436.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{drQuantile}}
#' @export
drHexbin <- function(data, xVar, yVar, by = NULL, xTransFn = identity, yTransFn = identity, xRange = NULL, yRange = NULL, xbins = 30, shape = 1, params = NULL, packages = NULL, control = NULL) {

  if(is.null(xRange) || is.null(yRange)) {
    # we need to know the range of the variables, which we don't
    # when we have a transformed object - and can't update attributes
    # (transformed objects are meant to be intermediate objects anyway)
    if(inherits(data, "transformed")) {
      stop("Cannot run drQuantile() on a transformed divided data object without xRange or yRange being explicitly specified.  Please specify these or first call drPersist() on this data to make transformation persistent.", call. = FALSE)
    }

    if(class(summary(data))[1] == "logical")
      stop("Need to know the range of the variable to compute hexbins - please run updateAttributes on this data.")

    xRange <- summary(data)[[xVar]]$range
    yRange <- summary(data)[[yVar]]$range
  }

  xbnds <- xTransFn(xRange)
  ybnds <- yTransFn(yRange)

  tmpSub <- data[[1]][[2]]
  tmpBin <- hexbin(xTransFn(tmpSub[[xVar]]), yTransFn(tmpSub[[yVar]]), xbnds = xbnds, ybnds = ybnds, xbins = xbins, shape = shape)

  map <- expression({
    dat <- data.frame(data.table::rbindlist(map.values))

    if(is.null(by)) {
      inds <- list("1" = seq_len(nrow(dat)))
    } else {
      splits <- getCondCuts(dat[, by, drop = FALSE], by)
      inds <- split(seq_along(splits), splits)
    }
    indsNms <- names(inds)
    for(ii in seq_along(inds)) {
      tmp <- hexbin(xTransFn(dat[[xVar]][inds[[ii]]]), yTransFn(dat[[yVar]][inds[[ii]]]), xbnds = xbnds, ybnds = ybnds, xbins = xbins, shape = shape)

      collect(indsNms[ii],
        data.frame(count = tmp@count, xcm = tmp@xcm, ycm = tmp@ycm, cell = tmp@cell))
    }
  })

  reduce <- expression(pre = {
    res <- NULL
  }, reduce = {
    res <- data.frame(data.table::rbindlist(c(list(res), reduce.values)))
    res <- data.frame(data.table::rbindlist(by(res, res$cell, function(a) {
      tot <- sum(a$count)
      data.frame(count = tot,
        xcm = sum(a$xcm * a$count) / tot,
        ycm = sum(a$ycm * a$count) / tot,
        cell = a$cell[1])
    })))
  }, post = {
    collect(reduce.key, res)
  })

  parList <- list(
    xVar = xVar, yVar = yVar, by = by,
    xTransFn = xTransFn, yTransFn = yTransFn,
    xbnds = xbnds, ybnds = ybnds,
    xbins = xbins, shape = shape
  )

  res <- mrExec(data, map = map, reduce = reduce,
    params = c(params, parList),
    packages = unique(c(packages, "datadr", "data.table", "hexbin")),
    verbose = FALSE)

  if(is.null(by)) {
    res <- res[[1]][[2]]
    d <- tmpBin
    d@cell <- res$cell
    d@count <- res$count
    d@xcm <- res$xcm
    d@ycm <- res$ycm
    d@n <- sum(res$count)
    d@ncells <- nrow(res)
    res <- d
  } else {
    resKeys <- sapply(res, "[[", 1)
    res <- lapply(res, function(x) {
      d <- tmpBin
      d@cell <- x[[2]]$cell
      d@count <- x[[2]]$count
      d@xcm <- x[[2]]$xcm
      d@ycm <- x[[2]]$ycm
      d@n <- sum(x[[2]]$count)
      d@ncells <- nrow(x[[2]])
      d
    })
    names(res) <- resKeys
  }

  res
}

# map.values <- lapply(data[1:2], "[[", 2)
#
# getHb <- function(dat) {
#   tmp <- hexbin(xTransFn(dat[[xVar]]), yTransFn(dat[[yVar]]), xbnds = xbnds, ybnds = ybnds, xbins = xbins, shape = shape)
#   tmphc <- hcell2xy(tmp)
#   data.frame(x = tmphc$x, y = tmphc$y, count = tmp@count, xcm = tmp@xcm, ycm = tmp@ycm, cell = tmp@cell)
# }
#
# reduce.values <- list(
#   getHb(data.frame(rbindlist(lapply(data[1:2], "[[", 2)))),
#   getHb(data.frame(rbindlist(lapply(data[3:4], "[[", 2))))
# )
