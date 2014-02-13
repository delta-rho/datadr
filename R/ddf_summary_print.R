#' @S3method print ddfSummary
print.ddfSummary <- function(x, ...) {
   sNames <- names(x)
   pv <- x
   for(i in seq_along(x)) {
      pv[[i]] <- getPrintVals(pv[[i]])
      vnc <- max(nchar(pv[[i]]))
      nnc <- nchar(sNames[i])
      pad <- floor(abs((nnc - vnc) / 2))
      if(nnc > vnc) {
         pv[[i]] <- paste(paste(rep(" ", pad), collapse=""), pv[[i]], sep="")
      } else {
         sNames[i] <- paste(paste(rep(" ", pad), collapse=""), sNames[i], sep="")
      }
      dashes <- paste(c(rep("-", max(c(vnc, nnc))), " "), collapse="")
      pv[[i]] <- c(dashes, pv[[i]], dashes)
   }
   
   a <- do.call(cbind, pv)
   dimnames(a) <- list(rep("", 9), sNames)
   class(a) <- "table"
   print(a)
}

getPrintVals <- function(x)
   UseMethod("getPrintVals", x)

getPrintVals.ddfSummNumeric <- function(x) {
   x <- c(nna=x$nna, min=x$range[1], max=x$range[2], x$stats)
   x$var <- sqrt(x$var)
   names(x)[names(x) == "nna"] <- "missing"
   names(x)[names(x) == "var"] <- "std dev"
   names <- names(x)
   vals <- sapply(x, format)

   nn <- max(nchar(names))
   vn <- max(nchar(vals))

   np <- sprintf(paste("%", nn, "s : ", sep=""), names)
   vp <- sprintf(paste("%", vn, "s", sep=""), vals)
   paste(np, vp, sep="")
}

getPrintVals.ddfSummFactor <- function(x) {
   nShow <- 4
   maxNchar <- 50
   
   nLevels <- ifelse(x$complete, as.character(nrow(x$freqTable)), "10000+")
   
   res <- c(
      paste("        levels :", nLevels), 
      paste("       missing :", x$nna), 
      paste("> freqTable head <"))
   
   n <- nrow(x$freqTable)
   names <- x$freqTable$var[seq_len(min(c(n, nShow)))]
   vals <- sapply(x$freqTable$Freq[seq_len(min(c(n, nShow)))], format)
   
   nn <- max(nchar(names))
   vn <- max(nchar(vals))
   
   if(nn > maxNchar) {
      ind <- which(nchar(names) > maxNchar)
      names[ind] <- paste(substr(names[ind], 1, maxNchar - 3), "...", sep="")
      nn <- maxNchar
   }
   
   np <- sprintf(paste("%", nn, "s : ", sep=""), names)
   vp <- sprintf(paste("%", vn, "s", sep=""), vals)
   
   res <- c(res, paste(np, vp, sep=""))
   
   # center the "freqTable head" label
   mm <- max(nchar(res))
   df <- floor((mm - nchar(res[3])) / 2)
   if(df > 0)
      res[3] <- paste(paste(rep(" ", df), collapse=""), res[3], sep="")
   
   if(length(names) < nShow)
      res <- c(res, rep(" ", nShow - length(names)))
   
   res
}
