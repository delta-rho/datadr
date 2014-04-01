############################################################################
### helper functions for updateAttributes
############################################################################

#' Functions to Compute Summary Statistics in MapReduce
#' 
#' Functions that are used to tabulate categorical variables and compute moments for numeric variables inside through the MapReduce framework.  Used in \code{\link{updateAttributes}}.

#' @param formula a formula to be used in \code{\link{xtabs}}
#' @param data a subset of a 'ddf' object
#' @param maxUnique the maximum number of unique combinations of variables to obtaion tabulations for.  This is meant to help against cases where a variable in the formula has a very large number of levels, to the point that it is not meaningful to tabulate and is too computationally burdonsome.  If \code{NULL}, it is ignored.  If a positive number, only the top and bottom \code{maxUnique} tabulations by frequency are kept.
#' 
#' @export
#' @rdname mrSummaryStats
tabulateMap <- function(formula, data) {
   tmp <- xtabs(formula, data = data)
   if(length(tmp) > 0) {
      return(as.data.frame(tmp, stringsAsFactors = FALSE))
   } else {
      return(NULL)
   }
}

#' @param result,reduce.values inconsequential \code{tabulateReduce} parameters
#' @export
#' @rdname mrSummaryStats
tabulateReduce <- function(result, reduce.values, maxUnique = NULL) {
   suppressWarnings(suppressMessages(require(data.table)))
   tmp <- data.frame(rbindlist(reduce.values))
   tmp <- rbind(result, tmp)
   tmp <- xtabs(Freq ~ ., data=tmp)
   if(length(tmp) > 0) {
      tmp <- as.data.frame(tmp, stringsAsFactors=FALSE)
      # only tabulate top and bottom maxUnique values
      idx <- order(tmp$Freq, decreasing = TRUE)
      if(is.null(maxUnique)) {
         return(tmp[idx,])
      } else {
         if(nrow(tmp) > maxUnique) {
            return(tmp[c(head(idx, maxUnique / 2), tail(idx, maxUnique / 2)),])
         } else {
            return(tmp[idx,])
         }
      }      
   } else {
      return(NULL)
   }
}

#' @param y,order,na.rm inconsequential \code{calculateMoments} parameters
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

#' @param m1,m2 inconsequential \code{combineMoments} parameters
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

#' @param \ldots inconsequential parameters
#' @export
#' @rdname mrSummaryStats
combineMultipleMoments <- function(...) {
   args <- list(...)
   
   # get rid of NAs
   l1 <- which(sapply(args, length) == 1)
   if(length(l1) > 0) {
      ind <- which(is.na(unlist(args[l1])))
      if(length(ind) > 0)
         args <- args[-l1[ind]]
   }
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

#' @param m inconsequential \code{moments2statistics} parameters
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
