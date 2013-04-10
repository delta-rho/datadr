# check to see if file exists on HDFS
existsOnHDFS <- function(...) {
   params <- list(...)
   path <- rhabsolute.hdfs.path(paste(params, collapse="/"))
   res <- try(rhls(path), silent=TRUE)
   if(inherits(res, "try-error")) {
      return(FALSE)      
   } else {
      return(TRUE)
   }
}

# print size provided in bytes in nice format
prettySize <- function(x) {
   units <- c("KB", "MB", "GB", "TB")
   xp <- min(which(floor((x / (1024^(1:4))) %% 1024) == 0)) - 1
   if(is.infinite(xp))
      xp <- 4
   xp <- max(xp, 1)
   
   paste(round(x / (1024^xp), 2), units[xp])
}

#### functions to read and write rh attributes
# internal
loadRhDataAttr <- function(loc) {
   rhload(paste(loc, "/_rh_meta/rhDataAttr.Rdata", sep=""))
   dat
}

# internal
saveRhDataAttr <- function(dat, loc) {
   rhsave(dat, file=paste(loc, "/_rh_meta/rhDataAttr.Rdata", sep=""))
}

# internal
saveRhDivAttr <- function(dat, loc) {
   rhsave(dat, file=paste(loc, "/_rh_meta/rhDivAttr.Rdata", sep=""))
}

# internal
loadRhDivAttr <- function(loc) {
   rhload(paste(loc, "/_rh_meta/rhDivAttr.Rdata", sep=""))
   dat
}

# internal
loadRhDFattr <- function(loc) {
   rhload(paste(loc, "/_rh_meta/rhDFattr.Rdata", sep=""))   
   dat
}

# internal
saveRhDFattr <- function(dat, loc) {
   rhsave(dat, file=paste(loc, "/_rh_meta/rhDFattr.Rdata", sep=""))
}


#### functions used in divide

# internal
orderData <- function(dat, orderBy) {
   orderCols <- lapply(orderBy, function(x) {
      if(length(x) == 1) {
         var <- x
         ord <- "asc"
      } else {
         var <- x[1]
         ord <- x[2]
      }
      if(ord == "desc") {
         return(-xtfrm(dat[,var]))
      } else {
         return(dat[,var])
      }
   })
   orderIndex <- do.call(order, orderCols) 
   dat[orderIndex,]              
}

# internal
getUID <- function(id=Sys.getenv("mapred.task.id")) {
  a <- strsplit(id, "_")[[1]]
  a <- as.numeric(a[c(2, 3, 5)])
}

# perhaps use key as a basis for seed:
# sum(strtoi(charToRaw(digest(key)), 16L))

# internal
setupRNGStream <- function(iseed) {
   ## Modified from clustersetupRNGstream in library(parallel)
   library(parallel)
   RNGkind("L'Ecuyer-CMRG")
   set.seed(iseed)
   current.task.number <- getUID()[3]
   seed <- .Random.seed
   if(current.task.number > 1) {
      for(i in 2:current.task.number)
         seed <- nextRNGStream(seed)
      assign(".Random.seed", seed, envir = .GlobalEnv)
   }
}


#### functions used in print methods

NArender <- function(x, txt, alt) {
   if(any(is.na(x))) {
      return(alt)
   } else {
      return(txt)
   }
}

sourceDataText <- function(x) {
   n <- length(x)
   if(length(x) > 1)
      x <- c(x[1], paste("and", n-1, "more..."))
   paste(x, collapse=", ")
}

condDivText <- function(x) {
   paste("Conditioning variables: ", paste(x$vars, collapse=", "), sep="")
}

rrDivText <- function(x) {
   paste("Approx. number of rows in each division: ", x$nrows, sep="")
}

printVars <- function(x) {
   names <- names(x)
   types <- substr(unlist(x), 1, 3)
   paste(paste(names, "(", types, ")", sep=""), collapse=", ")
}

printTrans <- function(x) {
   text <- "custom user-defined function - see dat$trans and see dat$transExample for example of a subset post-transformation"
   if(identical(x, identity)) {
      text <- "identity transformation (original data is a data.frame)"
   } else if(identical(x, as.data.frame)) {
      text <- "data is coerced into a data.frame using as.data.frame - see dat$transExample for example of a subset post-transformation"
   }
}

buildPrintTable <- function(x, attrList, class) {
   xnm <- intersect(names(x), names(attrList))
   xnm2 <- paste("*", xnm)
   vals <- do.call(c, lapply(xnm, function(a) {
      cur <- attrList[[a]]
      cur$render(x[[a]])
   }))
   
   namewidth <- max(nchar(xnm2)) + 1
   valwidth <- min(max(nchar(vals)), getOption("width") - namewidth - 1)

   fmt <- paste("%-", namewidth, "s: %s", sep="")
   fmt2 <- paste("%-", namewidth, "s: %s", sep="")
   
   tab <- do.call(c, lapply(seq_along(xnm), function(i) {
      sprintf(fmt, xnm2[i], vals[i])
   }))
   
   header <- c(
      sprintf(fmt, paste("'", class, "' attr", sep=""), "value"),
      paste(rep("-", namewidth + valwidth + 2), collapse="")
   )
   paste(c(header, tab), collapse="\n")
}
