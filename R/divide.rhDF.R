#' Divide a RHIPE 'rhDF' Object
#'
#' Divide a RHIPE 'rhDF' object into subsets based on different criteria
#'
#' @param data an object of class 'rhDF'
#' @param by specification of how to divide the data - conditional (factor-level or shingles), random replicate, or near-exact replicate -- see details
#' @param orderBy within each division, how should the data be sorted?  Either a vector of variable names (which will be sorted in ascending order) or a list such as the following: \code{list(c("var1", "desc"), c("var2", "asc"))}, which would sort the data first by \code{var1} in descending order, and then by \code{var2} in ascending order
#' @param output where on HDFS the output data should reside
#' @param mapred mapreduce-specific parameters to send to RHIPE job that creates the division (see \code{\link{rhwatch}})
#' @param postTrans a transformation function (if desired) to apply to each final subset
#' @param trans transformation function to coerce data transformed with postTrans back into a data.frame, so the result can behave like an object of class 'rhDF' (if desired)
#' 
#' @return an object of class 'rhData', 'rhDF', and 'rhDiv' which points to the divided data on HDFS
#' 
#' @details The division methods this function will support include conditioning variable division for factors (implemented -- see \code{\link{condDiv}}), conditioning variable division for numerical variables through shingles, random replicate, and near-exact replicate.  If \code{by} is a vector of variable names, the data will be divided by these variables.  Alternatively, this can be specified by e.g.  \code{condDiv(c("var1", "var2"))}.  
#' 
#' @references
#' \itemize{
#'   \item \url{http://www.datadr.org}
#'   \item \href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha, S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., & Cleveland, W. S. (2012). Large complex data: divide and recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.}
#' }
#' 
#' @author Ryan Hafen
#' @seealso \code{\link{rhDF}}, \code{\link{condDiv}}, \code{\link{rrDiv}}
#' @examples
#' \dontrun{
#' 
#' }
#' @export
divide.rhDF <- function(data, by=NULL, orderBy=NULL, output=NULL, mapred=NULL, postTrans=NULL, trans=NULL, update=FALSE) {
   
   if(is.null(output))
      stop("Must provide location on HDFS for output data")
   
   if(is.null(trans))
      trans <- identity
      
   # validate "by"
   if(!is.list(by)) { # by should be a list
      by <- condDiv(by)
   }
   
   if(!inherits(by, "divSpecList")) {
      if(length(by) > 1)
         stop("Nested conditioning has not yet been implemented.")      
   }
   
   if(! by$type %in% c("condDiv", "rrDiv"))
      stop("Only 'condDiv' and 'rrDiv' divisions have been implemented.")

   n <- data$nrow
   if(is.null(n) && by$type == "rrDiv")
      stop("To do random replicate division, must know the total number of rows.  Call updateAttributes() on your data.")
   
   seed <- NULL
   if(by$type == "rrDiv")
      seed <- by$seed
   
   if(is.null(seed))
      seed <- as.integer(runif(1)*1000000)
      
   # TODO: validate "by" against examples
   
   # TODO: should have enough summary info to know if division will result in subsets that are too large.  Good to check for and warn about this.
   
   setup <- as.expression(bquote({
      suppressMessages(library(datadr))
   	seed <- .(seed)
      datadr:::setupRNGStream(seed)
   }))
   
   if(rhabsolute.hdfs.path(output) == data$loc) {
      if(by$type != "condDiv") {
         stop("\"output\" is the same as location of input data")
      } else {
         message("* \"output\" is same as location of input data.  Verifying on a subset that the input data already appears to be divided by the specified conditioning variables")
         
         tmp <- trans(divExample(data))
         
         # data has all columns of "by"
         if(all(by$vars %in% names(tmp))) {
            # all "by" columns have a single value
            if(!all(sapply(tmp[by$vars], function(x) length((unique(x))) == 1))) {
               stop("Columns of 'by' variables are not unique")
            }
         } else {
            stop("Not all 'by' variables present in data")
         }
         message("* Subset passed - only one subset was tested... continue at your own risk")
      }
   } else {
      map <- rhmap({
         # TODO: pass factor levels of any factor variables through and in the reduce, assign this, to preserve factor levels across subsets
         data <- trans(r)

         if(by$type == "condDiv") {
            splitVars <- by$vars
            cuts <- apply(data[,splitVars,drop=FALSE], 1, function(x) paste(paste(splitVars, "=", x, sep=""), collapse="|"))      
         } else if(by$type=="rrDiv") {
            # get the number of splits necessary for specified nrows
            nr <- by$nrows
            ndiv <- ceiling(n / nr)
            cuts <- paste("rr_", sample(1:ndiv, n, replace=TRUE), sep="")
         }

         cutDat <- split(data, cuts)
         cdn <- names(cutDat)

         for(i in seq_along(cutDat)) {
            rhcollect(cdn[i], cutDat[[i]])
         }
      })
      
      reduce <- expression(
         pre={
            res <- list()
         },
         reduce={
            res[[length(res) + 1]] <- reduce.values
         },
         post={
            # TODO: use rbindlist from data.table for speed
            # TODO: spill records to new key/value if too many rows?
            res <- do.call(rbind, unlist(res, recursive=FALSE))
            if(!is.null(orderBy)) {
               res <- orderData(res, orderBy)
            }
            # add conditioning variable current split vals
            if(by$type=="condDiv") {
               splitVars <- by$vars
               for(i in seq_along(res)) {
                  tmp <- res[1,splitVars, drop=FALSE]
                  tmp <- lapply(tmp, as.character)
                  tmp <- data.frame(tmp, stringsAsFactors=FALSE)
                  attr(res, "split") <- tmp
               }
            }
            
            rhcounter("rhDF", "nrow", nrow(res))
            rhcollect(reduce.key, res)
         }
      )
      
      # set write.job.info to TRUE
      wji <- rhoptions()$write.job.info
      rhoptions(write.job.info=TRUE)
      
      tmp <- rhwatch(
         setup=setup,
         map=map, 
         reduce=reduce, 
         input=rhfmt(data$loc, type=data$type),
         output=rhfmt(output, type="map"),
         mapred=mapred, 
         readback=FALSE, 
         parameters=list(
            by = by,
            n = n,
            orderBy = NULL,
            trans = data$trans,
            seed = seed
            # orderData = orderData,
            # getUID = getUID,
            # setupRNGStream = setupRNGStream
         )
      )

      # set write.job.info back to what it was
      rhoptions(write.job.info=wji)      
   }

   if(is.null(postTrans)) { # result is a data.frame
      res <- rhDF(output, type="map")
   } else {
      # check to see if it can either be 
      tmp <- try(rhDF(output, type="map", trans=postTrans))
      if(inherits(tmp, "try-error")) {
         res <- rhData(output, type="map")
      } else {
         res <- tmp         
      }
   }
   
   # add divBy to return object and make a meta file for it
   saveRhDivAttr(by, res$loc)
   
   res$divBy <- by
   classes <- class(res)
   classes <- classes[grepl("^rh", classes)]
   class(res) <- c(classes, "rhDiv", "list")
   
   if(update)
      res <- updateAttributes(res, mapred=mapred)
   
   res
}


