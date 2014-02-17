#' Sample Quantiles for 'ddf' Objects
#' 
#' Compute sample quantiles for 'ddf' objects
#' 
#' @param x a 'ddf' object
#' @param var the name of the variable to compute quantiles for
#' @param by the (optional) variable by which to group quantile computations
#' @param probs numeric vector of probabilities with values in [0-1]
#' @param transFn transformation to apply to variable prior to computing quantiles
#' @param nBins how many bins should the range of the variable be split into?
#' @param tails how many exact values at each tail should be retained?
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
#'    list("1", iris[1:10,]), list("2", iris[11:110,]), list("3", iris[111:150,])
#' )
#' # represent it as ddf
#' irisSplit <- ddf(irisSplit, update = TRUE)
#' 
#' # approximate quantiles over the divided data set
#' probs <- seq(0, 1, 0.005)
#' iq <- quantile(irisSplit, var = "Sepal.Length", tails = 0, probs = probs)
#' plot(iq$fval, iq$q)
#' 
#' # compare to the all-data quantile "type 1" result
#' plot(probs, quantile(iris$Sepal.Length, probs = probs, type = 1))
#' 
#' @method quantile ddf
#' @importFrom stats quantile
#' @export
quantile.ddf <- function(x, var, by = NULL, probs = seq(0, 1, 0.005), transFn = identity, nBins = 10000, tails = 100, control = NULL, ...) {
   # nBins <- 10000; tails <- 0; probs <- seq(0, 1, 0.0005); by <- "Species"; var <- "Sepal.Length"; x <- ldd; trans <- identity
   
   if(class(summary(x))[1] == "logical")
      stop("Need to know the range of the variable to compute quantiles - please run updateAttributes on this data.")
   
   rng <- transFn(summary(x)[[var]]$range)
   delta <- diff(rng) / (nBins - 1)
   cuts <- seq(rng[1] - delta / 2, rng[2] + delta/2, by=delta)
   mids <- seq(rng[1], rng[2], by=delta)
   
   map <- expression({
      v <- transFn(do.call(c, lapply(map.values, function(x) dfTrans(x)[, var])))
      if(is.null(by)) {
         by <- rep("1", length(v))
      } else {
         by <- do.call(c, lapply(map.values, function(x) as.character(dfTrans(x)[, by])))
      }
      ind <- split(seq_along(by), by)
      for(ii in ind) {
         vv <- v[ii]
         vv <- vv[!is.na(vv)]
         if(length(vv) > 0) {
            ord <- order(vv)

            cutTab <- as.data.frame(table(cut(vv, cuts, labels=FALSE)), responseName = "Freq", stringsAsFactors = FALSE)
            cutTab$Var1 <- as.integer(cutTab$Var1)

            for(i in 1:nrow(cutTab)) {
               collect(list(by[ii[1]], cutTab$Var1[i]), cutTab$Freq[i])
            }

            collect(list(by[ii[1]], "bot"), vv[head(ord, tails)])
            collect(list(by[ii[1]], "top"), vv[tail(ord, tails)])            
         }
      }
   })
   
   reduce <- expression(
      pre = {
         bot <- NULL
         top <- NULL
         sum <- 0
      }, reduce = {
         if(reduce.key[[2]] == "bot") {
            bot <- head(sort(c(bot, do.call(c, reduce.values))), tails)
         } else if(reduce.key[[2]] == "top") {
            top <- tail(sort(c(top, do.call(c, reduce.values))), tails)            
         } else {
            sum <- sum + sum(unlist(reduce.values))
         }
      }, post = {
         if(reduce.key[[2]] == "bot") {
            collect(reduce.key, bot)
         } else if(reduce.key[[2]] == "top") {
            collect(reduce.key, top)
         } else {
            collect(reduce.key, sum)
         }
      }
   )
   
   mrRes <- mrExec(x,
      map = map,
      reduce = reduce,
      params = list(
         transFn = transFn,
         dfTrans = getAttribute(x, "transFn"),
         var = var,
         by = by,
         cuts = cuts,
         tails = tails
      ),
      control = control
   )
   
   # put things together...
   mrRes <- getAttribute(mrRes, "conn")$data
   groups <- sapply(mrRes, function(x) x[[1]][[1]])
   
   ind <- split(seq_along(groups), groups)
   
   if(length(ind) == 1) {
      res <- constructQuants(mrRes, probs, tails, mids)
   } else {
      res <- lapply(seq_along(ind), function(i) {
         data.frame(
            constructQuants(mrRes[ind[[i]]], probs, tails, mids),
            group = groups[ind[[i]][1]],
            stringsAsFactors = FALSE
         )
      })
      res <- do.call(rbind, res)
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
   
   fn <- approxfun(quants$cpct, quants$q, method="constant", f=1, rule=2)
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
      
      res <- subset(res, fval > max(botDf$fval) & fval < min(topDf$fval))
      res <- rbind(botDf, res, topDf)
   }
   res
}
