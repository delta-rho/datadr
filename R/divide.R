#' Divide a Distributed Data Object
#'
#' Divide a ddo/ddf object into subsets based on different criteria
#' 
#' @param data an object of class "ddf" or "ddo" - in the latter case, need to specify \code{preTransFn} to coerce each subset into a data frame
#' @param by specification of how to divide the data - conditional (factor-level or shingles), random replicate, or near-exact replicate (to come) -- see details
#' @param bsvFn a function to be applied to each subset that returns a list of between subset variables (BSVs)
#' @param output a "kvConnection" object indicating where the output data should reside (see \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If \code{NULL} (default), output will be an in-memory "ddo" object.
#' @param spill integer telling the division method how many lines of data should be collected until spilling over into a new key-value pair
#' @param filterFn a function that is applied to each candidate output key-value pair to determine whether it should be (if returns \code{TRUE}) part of the resulting division
#' @param preTransFn a transformation function (if desired) to applied to eah subset prior to division
#' @param postTransFn a transformation function (if desired) to apply to each post-division subset
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{\link{rhwatch}} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' @param update should a MapReduce job be run to obtain additional attributes for the result data prior to returning?
#' @param verbose logical - print messages about what is being done
#' 
#' @return an object of class "ddf" if the resulting subsets are data frames.  Otherwise, an object of class "ddo".
#' 
#' @details The division methods this function will support include conditioning variable division for factors (implemented -- see \code{\link{condDiv}}), conditioning variable division for numerical variables through shingles, random replicate (implemented -- see \code{\link{rrDiv}}), and near-exact replicate.  If \code{by} is a vector of variable names, the data will be divided by these variables.  Alternatively, this can be specified by e.g.  \code{condDiv(c("var1", "var2"))}.  
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' @seealso \code{\link{recombine}}, \code{\link{ddo}}, \code{\link{ddf}}, \code{\link{condDiv}}, \code{\link{rrDiv}}
#' @examples
#' \dontrun{
#' 
#' }
#' @export
divide <- function(data, 
   by = NULL, 
   spill = 1000000,
   filterFn = NULL, 
   bsvFn = NULL,
   output = NULL,
   preTransFn = NULL,
   # blockPreTransFn = NULL,
   postTransFn = NULL,
   control = NULL,
   update = FALSE,
   verbose = TRUE
) {
   # data <- ddf(localDiskConn("~/Desktop/asdf")); by <- "Species"; bsvFn <- NULL; preTransFn <- NULL; postTransFn <- NULL; output <- localDiskConn("~/Desktop/asdf3"); control <- NULL; update <- FALSE
   
   if(!inherits(data, "ddf")) {
      if(verbose)
         message("* Input data is not 'ddf' - attempting to cast it as such")
      data <- ddf(data)
   }
   
   if(verbose)
      message("* Verifying parameters...")
   
   if(!is.list(by)) { # by should be a list
      by <- condDiv(by)
   }
   
   seed <- NULL
   if(by$type == "rrDiv")
      seed <- by$seed
   
   if(is.null(seed))
      seed <- as.integer(runif(1)*1000000)
   
   if(is.null(preTransFn))
      preTransFn <- identity
   
   if(is.null(filterFn)) {
      filterFn <- function(x) TRUE
      environment(filterFn) <- .GlobalEnv
   }
   
   # get an example of what a subset will look like when it is to be divided
   ex <- kvExample(data, transform=TRUE)
   ex <- kvApply(preTransFn, ex, returnKV=TRUE)
   
   # make sure the division specification is good
   # and get any parameters that need to be sent to dfSplit
   by$specVars <- validateDivSpec(by, data, ex)
   
   if(!is.null(bsvFn)) {
      # Even though bsvFn is called on subsets *after* division
      # it is good to validate here since an error after would
      # be a waste of a lot of computation only to abort.
      # Since bsvFn is applied *before* postTransFn() is called
      # it should work on an input subset after preTransFn().
      
      validateBsvFn(ex, bsvFn, verbose=TRUE)
   }
   if(verbose)
      message("* Applying division...")
   
   parList <- list(
      by = by,
      transFn = getAttribute(data, "transFn"),
      preTransFn = preTransFn,
      postTransFn = postTransFn,
      seed = seed,
      bsvFn = bsvFn,
      MAX_ROWS = spill,
      filterFn = filterFn
   )
   
   globalVars <- unique(c(
      drFindGlobals(preTransFn),
      drFindGlobals(postTransFn),
      drFindGlobals(bsvFn),
      drFindGlobals(filterFn)
   ))
   globalVarList <- getGlobalVarList(globalVars, parent.frame())
   if(length(globalVarList) > 0)
      parList <- c(parList, globalVarList)
   
   if(! "package:datadr" %in% search()) {
      if(verbose)
         message("* ---- running dev version - sending datadr functions to mr job")
      parList <- c(parList, list(
         dfSplit = dfSplit,
         bsv = bsv,
         kvApply = kvApply,
         getCuts = getCuts,
         getCuts.condDiv = getCuts.condDiv,
         getCuts.rrDiv = getCuts.rrDiv
      ))
      
      setup <- as.expression(bquote({
         suppressWarnings(suppressMessages(library(data.table)))
      	seed <- .(seed)
         # datadr:::setupRNGStream(seed)
      }))
   } else {
      setup <- as.expression(bquote({
         suppressWarnings(suppressMessages(library(datadr)))
      	seed <- .(seed)
         # datadr:::setupRNGStream(seed)
      }))
   }
   
   map <- expression({
      for(i in seq_along(map.values)) {
         curKV <- kvApply(preTransFn,
            kvApply(transFn, list(map.keys[[i]], map.values[[i]]),   
               returnKV=TRUE), returnKV=TRUE)
         
         cutDat <- dfSplit(curKV[[2]], by, seed)
         cdn <- names(cutDat)
         
         for(i in seq_along(cutDat)) {
            collect(cdn[i], cutDat[[i]])
         }
         
         # counter("datadr", "Divide map k/v processed", 1)
         # counter("datadr", "Divide map k/v emitted", length(cutDat))
         # counter("datadr", "Divide map pre-division bytes", as.integer(object.size(r)))
         # counter("datadr", "Divide map post-division bytes", sum(sapply(cutDat, function(x) as.integer(object.size(x)))))
      }
   })
   
   reduce <- expression(
      pre={
         # df, nr, and count used for "spill"
         df <- list()
         nr <- 0
         spillCount <- 0
         
         # counter("datadr", "Divide reduce k/v processed", 1)
      },
      reduce={
         df[[length(df) + 1]] <- reduce.values
         nr <- nr + sum(sapply(reduce.values, nrow))
         if(nr > MAX_ROWS) {
            # this df is ready to go
            
            for(ii in seq_len(floor(nr / MAX_ROWS))) {
               spillCount <- spillCount + 1
               df <- data.frame(rbindlist(unlist(df, recursive = FALSE)))
               # do what is needed and collect
               if(kvApply(filterFn, list(reduce.key, df))) {
                  # put in div-specific attr stuff
                  res <- addSplitAttrs(df[1:MAX_ROWS,], bsvFn, by, postTransFn)

                  # counter("datadr", "spilled", 1)
                  collect(paste(reduce.key, spillCount, sep="_"), res)               
               }
               # now continue to work on what is left over
               df <- list(list(df[c((MAX_ROWS + 1):nr),]))
               nr <- nr - MAX_ROWS
            }
         }
      },
      post={
         df <- data.frame(rbindlist(unlist(df, recursive = FALSE)))
         # if we never reached MAX_ROWS, don't append count to key
         if(spillCount > 0)
            reduce.key <- paste(reduce.key, spillCount + 1, sep="_")
         
         if(kvApply(filterFn, list(reduce.key, df))) {
            # put in div-specific attr stuff
            res <- addSplitAttrs(df, bsvFn, by, postTransFn)
            collect(reduce.key, res)               
         }
      }
   )
   
   res <- mrExec(data,
      setup=setup,
      map=map, 
      reduce=reduce, 
      output=output,
      params=parList
   )
   
   # return ddo or ddf object
   tmp <- try(ddf(res, update=update, verbose=FALSE), silent=TRUE)
   if(inherits(tmp, "try-error")) {
      res <- ddo(getAttribute(res, "conn"), update=update, verbose=FALSE)
   } else {
      res <- tmp
   }
   
   # add an attribute specifying how it was divided
   res <- setAttributes(res, list(div=list(divBy=by)))
   
   # add bsv attributes
   if(!is.null(bsvFn)) {
      desc <- getBsvDesc(kvExample(data)[[2]], bsvFn)
      tmp <- list(bsvFn = bsvFn, bsvDesc = desc)
      class(tmp) <- c("bsvInfo", "list")
      res <- setAttributes(res, list(bsvInfo = tmp))
   }
   
   res
}

#' Get Between Subset Variable
#' 
#' For a given key-value pair, get a BSV variable value by name (if present)
#' @param x a key-value pair or a value
#' @param name the name of the BSV to get
#' @export
getBsv <- function(x, name) {
   if(inherits(x, "divValue")) {
      return(attr(x, "bsv")[[name]])
   } else if(inherits(x[[2]], "divValue")) {
      return(attr(x[[2]], "bsv")[[name]])      
   } else {
      return(NULL)
   }
}

#' Get Between Subset Variables
#' 
#' For a given key-value pair, exract all BSVs
#' @param x a key-value pair or a value
#' @export
getBsvs <- function(x) {
   if(inherits(x, "divValue")) {
      return(attr(x, "bsv"))
   } else if(inherits(x[[2]], "divValue")) {
      return(attr(x[[2]], "bsv"))      
   } else {
      return(NULL)
   }
}

#' Extract "Split" Variable
#' 
#' For a given key-value pair or value, get a split variable value by name, if present (split variables are variables that define how the data was divided).
#' @param x a key-value pair or a value
#' @param name the name of the split variable to get
#' @export
getSplitVar <- function(x, name) {
   if(inherits(x, "divValue")) {
      return(attr(x, "split")[[name]])
   } else if(inherits(x[[2]], "divValue")) {
      return(attr(x[[2]], "split")[[name]])      
   } else {
      return(NULL)
   }
}

#' Extract "Split" Variables
#' 
#' For a given k/v pair or value, exract all split variables (split variables are variables that define how the data was divided).
#' @param x a key-value pair or a value
#' @export
getSplitVars <- function(x) {
   if(inherits(x, "divValue")) {
      return(as.list(attr(x, "split")))
   } else if(inherits(x[[2]], "divValue")) {
      return(as.list(attr(x[[2]], "split")))
   } else {
      return(NULL)
   }
}

# take a data frame (or one that becomes a data frame with preTransFn)
# and split it according to "by" and return a named list
# (this is meant to be called in parallel)

#' Functions used in divide()
#' #rdname divideInternals
#' @export
dfSplit <- function(curDF, by, seed) {   
   # remove factor levels, if any
   # TODO: keep track of factor levels
   factorInd <- which(sapply(curDF, is.factor))
   for(i in seq_along(factorInd)) {
      curDF[[factorInd[i]]] <- as.character(curDF[[factorInd[i]]])
   }
   
   split(curDF, getCuts(by, curDF))
}

#' #rdname divideInternals
#' @export
addSplitAttrs <- function(curSplit, bsvFn, by, postTransFn=NULL) {
   bsvs <- NULL
   
   # BSVs are applied before postTrans
   if(!is.null(bsvFn)) {
      bsvs <- stripBsvAttr(bsvFn(curSplit))
   }
   
   splitAttr <- NULL
   if(by$type=="condDiv") {
      splitVars <- by$vars
      splitAttr <- curSplit[1, splitVars, drop=FALSE]
   }
   
   if(!is.null(postTransFn)) {
      curSplit <- postTransFn(curSplit)
   } else {
      if(by$type=="condDiv") {
         # remove columns for split variables
         curSplit <- curSplit[,setdiff(names(curSplit), by$vars), drop=FALSE]
      }
   }
   
   attr(curSplit, "bsv") <- bsvs
   attr(curSplit, "split") <- splitAttr
   class(curSplit) <- c("divValue", class(curSplit))
   
   curSplit
}

