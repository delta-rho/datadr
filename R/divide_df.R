
getDivideDF <- function(data, by, postTransFn, bsvFn, update = FALSE) {
   d <- data.table(data)
   # convert factors to strings
   factorInd <- which(sapply(d, is.factor))
   for(i in seq_along(factorInd)) {
      d[[factorInd[i]]] <- as.character(d[[factorInd[i]]])
   }
   
   nms <- names(d)
   # d$i <- seq_len(nrow(b))
   setkeyv(d, by$vars)
   
   keyCols <- format(as.matrix(data.frame(unique(d))[,by$vars,drop=FALSE]), scientific = FALSE, trim = TRUE, justify = "none")
   keys <- apply(keyCols, 1, function(x) paste(paste(by$vars, "=", x, sep=""), collapse="|"))
   
   res <- vector("list", length(keys))
   
   ii <- 0
   ff <- function(dt) {
      ii <<- ii + 1
      res[[ii]] <<- list()
      res[[ii]]$key <<- keys[ii]
      res[[ii]]$value <<- addSplitAttrs(data.frame(dt), bsvFn, by, postTransFn)
      1
   }
   
   tmp <- d[, ff(.SD), by = key(d), .SDcols = nms]
   
   if(is.data.frame(res[[1]]$value)) {
      res <- ddf(res)      
   } else {
      res <- ddo(res)
   }
   
   if(update)
      res <- updateAttributes(res)
   
   # add an attribute specifying how it was divided
   res <- setAttributes(res, list(div = list(divBy = by)))
   
   # add bsv attributes
   if(!is.null(bsvFn)) {
      desc <- getBsvDesc(kvExample(res)[[2]], bsvFn)
      tmp <- list(bsvFn = bsvFn, bsvDesc = desc)
      class(tmp) <- c("bsvInfo", "list")
      res <- setAttributes(res, list(bsvInfo = tmp))
   }
   
   res
}
