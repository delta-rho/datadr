# take a data.frame (or one that becomes a data.frame with preTrans)
# and split it according to "by" and return a named list
# (this is meant to be called in parallel)
dfSplit <- function(data, by, seed, preTrans) {
   if(!is.null(preTrans)) {
      data <- preTrans(data)
   }
   
   # remove factor levels, if any
   # TODO: keep track of factor levels
   factorInd <- which(sapply(data, is.factor))
   for(i in seq_along(factorInd)) {
      data[[factorInd[i]]] <- as.character(data[[factorInd[i]]])
   }
   
   if(by$type == "condDiv") {
      splitVars <- by$vars
      cuts <- apply(data[,splitVars,drop=FALSE], 1, function(x) paste(paste(splitVars, "=", x, sep=""), collapse="|"))      
      
      uniqueValues <- lapply(splitVars, function(x) {
         tmp <- unique(data[,x])
         if(class(tmp) == "factor")
            tmp <- as.character(tmp)
         tmp
      })
   } else if(by$type=="rrDiv") {
      splitVars <- "__random__"
      # get the number of splits necessary for specified nrows
      n <- nrow(data)
      nr <- by$nrows
      ndiv <- round(n / nr, 0)
      if(!is.null(by$seed)) set.seed(by$seed)
      # cuts <- paste("rr_", sample(1:ndiv, n, replace=TRUE), sep="")
      cuts <- paste("rr_", rep(1:ndiv, each=ceiling(n / ndiv))[1:n][sample(1:n, replace=FALSE)], sep="")
   }
   
   tmp <- split(data, cuts)
}

addSplitAttrs <- function(dat, bsv, by, postTrans=NULL) {
   bsvs <- NULL
   if(!is.null(bsv)) {
      if(is.character(bsv)) {
         # TODO: check 
         bsvs <- dat[1, intersect(names(dat), bsv)]
      } else if(is.function(bsv)) {
         bsvs <- bsv(dat)
      }
   }
   
   splitAttr <- NULL
   if(by$type=="condDiv") {
      splitVars <- by$vars
      splitAttr <- dat[1, splitVars, drop=FALSE]
      splitAttr <- lapply(splitAttr, as.character)
      splitAttr <- data.frame(splitAttr, stringsAsFactors=FALSE)
   }
   
   if(!is.null(postTrans)) {
      dat <- postTrans(dat)
   } else {
      if(by$type=="condDiv") {
         dat <- dat[,setdiff(names(dat), by$vars)]
      }
   }
   
   attr(dat, "bsv") <- bsvs
   attr(dat, "split") <- splitAttr
   class(dat) <- c("div.subset", "data.frame")
   
   dat
}
