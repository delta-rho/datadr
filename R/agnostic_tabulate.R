############################################################################
### helper functions for updateAttributes
############################################################################

#' Functions to Compute Summary Statistics in MapReduce
#' 
#' Functions that are used to tabulate categorical variables and compute moments for numeric variables inside through the MapReduce framework.  Used in \code{\link{updateAttributes}}.
#' 
#' @export
#' @rdname mrSummaryStats
tabulateMap <- function(formula, data) {
   as.data.frame(xtabs(formula, data=data), stringsAsFactors=FALSE)
}

#' @export
#' @rdname mrSummaryStats
tabulateReduce <- function(result, reduce.values) {
   suppressWarnings(suppressMessages(require(data.table)))
   tmp <- data.frame(rbindlist(reduce.values))
   tmp <- rbind(result, tmp)
   tmp <- as.data.frame(xtabs(Freq ~ ., data=tmp), stringsAsFactors=FALSE)
   # only tabulate top and bottom 10000 unique values
   if(nrow(tmp) > 10000) {
      idx <- order(tmp$Freq, tmp$var)
      return(tmp[c(head(idx, 5000), tail(idx, 5000)),])
   } else {
      return(tmp)
   }
}

#' @export
#' @rdname mrSummaryStats
calculateMoments <- function(y, order=1, na.rm=TRUE) {
   if(na.rm)
      y <- y[!is.na(y)]
      
   if(length(y)==0)
      return(NA)

   # res is a list containing each moment and the number of observations
   res <- list()
   length(res) <- order + 1
   res[[1]] <- mean(y, na.rm=TRUE)
   if(order > 1) {
      for(i in 2:order)
         res[[i]] <- sum((y - res[[1]])^i, na.rm=TRUE)
   }
   res[[order + 1]] <- as.numeric(length(y))
   names(res) <- c(paste("M", 1:order, sep=""), "n")
   res
}

#' @export
#' @rdname mrSummaryStats
combineMoments <- function(m1, m2) {
   if(length(m1) != length(m2))
      stop("objects not of the same length")
      # maybe should just give a warning and do the minimum
   
   order <- length(m1) - 1
   n <- m1$n + m2$n
   delta <- m2[[1]] - m1[[1]]
   res <- list()
   res[[1]] <- m1[[1]] + m2$n * (m2[[1]] - m1[[1]]) / n
   if(order > 1) {
      for(p in 2:order) {
         res[[p]] <- m1[[p]] + m2[[p]] + 
            ifelse(p > 2, sum(sapply(seq_len(p-2), function(k) {
               choose(p, k) * ((-m2$n / n)^k * m1[[p-k]] + (m1$n / n)^k * m2[[p-k]]) * delta^k
            })), 0) +
            (m1$n * m2$n / n * delta)^p * (1 / m2$n^(p-1) - (-1 / m1$n)^(p-1))
      }
   }
   res[[order + 1]] <- n
   names(res) <- c(paste("M", 1:order, sep=""), "n")
   res
}

#' @export
#' @rdname mrSummaryStats
combineMultipleMoments <- function(...) {
   args <- list(...)
   nArgs <- length(args)
   
   if(nArgs < 2) {
      return(args[[1]])
   }

   # TODO: paralellize this instead of doing it sequentially
   result <- combineMoments(args[[1]], args[[2]])
   for(i in seq_len(nArgs - 2)) {
      result <- combineMoments(args[[i + 2]], result)
   }
   result
}

#' @export
#' @rdname mrSummaryStats
moments2statistics <- function(m) {
   order <- length(m) - 1
   n <- m$n
   list(
      mean=m[[1]], 
      var=m[[2]]/(n-1), 
      skewness=(m[[3]] / n) / (m[[2]] / n)^(3/2),
      kurtosis=n * m[[4]] / m[[2]]^2
   )
}
