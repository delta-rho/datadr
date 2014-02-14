#' Print a "ddo" or "ddf" Object
#' 
#' Print an overview of attributes of distributed data objects (ddo) or distributed data frames (ddf)
#' 
#' @param x object to be printed
#' @param \ldots additional arguments
#' @author Ryan Hafen
#' @export
#' @method print ddo
print.ddo <- function(x, ...) {
   
   # objName <- as.character(substitute(x))
   
   objClass <- class(x)
   title <- paste("Distributed data object of class '", objClass[which(objClass=="ddo") + 1], "' with attributes: \n\n", sep="")
   
   ddoAttr <- getDr("ddoAttrPrintList")
   ddfAttr <- getDr("ddfAttrPrintList")
   
   # get all names that will be printed so that first column has same width for all tables
   nms <- names(getAttributes(x, names(ddoAttr))[["ddo"]])
   if("ddf" %in% objClass)
      nms <- c(nms, names(getAttributes(x, names(ddfAttr))[["ddf"]]))
      
   namewidth <- max(nchar(nms)) + 2
   options(namewidth=namewidth)
   
   ddoTab <- buildPrintTable(x, ddoAttr, "ddo", namewidth)
   ddoTab <- paste(ddoTab, "\n", sep="")
   ddfTab <- NULL
   if("ddf" %in% objClass) {
      ddfTab <- buildPrintTable(x, ddfAttr, "ddf", namewidth)
      ddfTab <- paste("\n", ddfTab, "\n", sep="")
   }
   
   ## text for div
   divTab <- NULL
   divAttr <- getAttribute(x, "div")
   if(!is.na(divAttr)) {
      divTab <- buildDivText(divAttr, getDr("divAttrPrintList"))      
      divTab <- paste("\n", divTab, sep="")
   }
   
   connText <- paste(capture.output(print(getAttribute(x, "conn"))), collapse="\n")
   
   cat("\n", title, ddoTab, ddfTab, divTab, "\n", connText, "\n", sep="")
}

# to get the name of the x:
# http://jackman.stanford.edu/blog/?p=800

# print size provided in bytes in nice format
prettySize <- function(x) {
   units <- c("KB", "MB", "GB", "TB")
   xp <- min(which(floor((x / (1024^(1:4))) %% 1024) == 0)) - 1
   if(is.infinite(xp))
      xp <- 4
   xp <- max(xp, 1)
   
   paste(round(x / (1024^xp), 2), units[xp])
}

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

printVars <- function(x, namewidth) {
   # fill the space with the variables we can print, and then say how many more there are
   # this is admittedly a bit ugly right now...
   valwidth <- getOption("width") - getOption("namewidth")
   names <- names(x)
   n <- length(names)
   extraTextLength <- nchar(paste("and", n, "more"))
   varswidth <- valwidth - extraTextLength
   types <- substr(unlist(x), 1, 3)
   names2 <- paste(names, "(", types, ")", sep="")
   nl <- cumsum(nchar(names2) + 2) - 2 # +2 for ", ", -2 for the last one not having ", "
   tmp <- which(nl <= varswidth)
   mx <- ifelse(length(tmp) > 0, max(tmp), 1)
   if(n - mx == 1 && nl[n] < valwidth) # if we are one away and can remove "and x more"
      mx <- n
   andMore <- ""
   if(mx < n)
      andMore <- paste(", and", n - mx, "more")
   paste(paste(names2[1:mx], collapse=", "), andMore, sep="")
}

printTrans <- function(x) {
   text <- "user-defined - see kvExample(dat, transform=TRUE)"
   if(identical(x, identity)) {
      text <- "identity (original data is a data frame)"
   } else if(identical(x, as.data.frame)) {
      text <- "as.data.frame - see kvExample(dat, transform=TRUE)"
   }
   text
}

buildPrintTable <- function(x, attrList, type, namewidth) {
   xAttr <- getAttributes(x, names(attrList))[[type]]
   xnm <- names(xAttr)
   xnm2 <- paste("", xnm)
   
   vals <- rep("", length(xnm))
   for(i in seq_along(vals)) {
      cur <- attrList[[xnm[i]]]
      vals[i] <- cur$render(xAttr[[xnm[i]]])
   }
   
   # namewidth <- max(nchar(xnm2)) + 1
   # valwidth <- min(max(nchar(vals)), getOption("width") - namewidth - 1)
   valwidth <- getOption("width") - namewidth - 1
   
   fmt <- paste("%-", namewidth, "s| %s", sep="")
   fmt2 <- paste("%-", namewidth, "s| %s", sep="")
   
   tab <- do.call(c, lapply(seq_along(xnm), function(i) {
      sprintf(fmt, xnm2[i], vals[i])
   }))
   
   header <- c(
      sprintf(fmt, paste("'", type, "' attribute", sep=""), "value"),
      paste(c(rep("-", namewidth), "+", rep("-", valwidth + 1)), collapse="")
   )
   paste(c(header, tab), collapse="\n")
}

buildDivText <- function(x, divAttrList) {
   divTab <- sapply(1, function(i) {
      curName <- x$divBy$type
      paste(
         # ifelse(i==1, "First-order", "Second-order"),
         "Division:\n",
         "  Type: ", divAttrList[[curName]]$desc, "\n",
         "    ", divAttrList[[curName]]$render(x$divBy), "\n",
         sep=""
      )
   })
   paste(divTab, collapse="\n")
}
