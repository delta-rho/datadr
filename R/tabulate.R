rhTabulateMap <- function(formula, data) {
   as.data.frame(xtabs(formula, data=data), stringsAsFactors=FALSE)
}

rhTabulateReduce <- function(result, reduce.values) {
   suppressWarnings(suppressMessages(require(data.table)))
   tmp <- data.frame(rbindlist(reduce.values))
   tmp <- rbind(result, tmp)
   as.data.frame(xtabs(Freq ~ ., data=tmp), stringsAsFactors=FALSE)
}

