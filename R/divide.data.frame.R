#' Divide a data.frame
#'
#' Divide a data.frame into subsets based on different criteria
#'
#' @param data data frame to be split 
#' @param by a vector of column names of \code{dat} to split by
#' @param orderBy within each division, how should the data be sorted?  Either a vector of variable names (which will be sorted in ascending order) or a list such as the following: \code{list(c("var1", "desc"), c("var2", "asc"))}, which would sort the data first by \code{var1} in descending order, and then by \code{var2} in ascending order
#' @param postTrans a transformation function (if desired) to apply to each subset after it has been formed
#' @param trans transformation function to coerce data transformed with postTrans back into a data.frame, so the result can behave like a data.frame (if desired)
#'
#' @return list of data frames split according to "by", of class "localDiv", with names denoting the current split
#' Objects of class "localDiv" are lists where each list element has the following:
#' \describe{
#'    \item{split:}{a list of the splitVars corresponding to the current split}
#'    \item{splitKey:}{the current splitVars concatenate into a single string}
#'    \item{data:}{The rows of the dat that correspond to the current split (with the splitVar columns omitted)}
#' } 
#'
#' @author Ryan Hafen
#' 
#' @seealso \code{\link{divJoin}}, \code{\link{divExample}}, \code{\link{SplitExampleKey}}
#'
#' @examples
#' 
#' irisSplit1 <- divide(iris, "Species")
#' 
#' @export
divide.data.frame <- function(data, by=NULL, orderBy=NULL, postTrans=NULL, trans=NULL) {
   
   if(!is.list(by)) { # by should be a list
      by <- condDiv(by)
   }
   
   if(is.null(trans))
      trans <- identity
   
   if(!inherits(by, "divSpecList")) {
      if(length(by) > 1)
         stop("Nested conditioning has not yet been implemented.")      
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
   } else {
      stop("Only 'condDiv' and 'rrDiv' divisions have been implemented.")
   }
   
   tmp <- split(data, cuts)
   
   # add conditioning variable current split vals
   if(by$type=="condDiv") {
      for(i in seq_along(tmp)) {
         tmp2 <- tmp[[i]][1,splitVars, drop=FALSE]
         tmp2 <- lapply(tmp2, as.character)
         tmp2 <- data.frame(tmp2, stringsAsFactors=FALSE)
         attr(tmp[[i]], "split") <- tmp2
      }
   }
   
   class(tmp) <- c("localDiv", "list")
   
   attr(tmp, "vars") <- lapply(tmp[[1]], class)
   attr(tmp, "totSize") <- as.numeric(object.size(tmp))
   attr(tmp, "ndiv") <- length(tmp)
   attr(tmp, "trans") <- trans
   attr(tmp, "nrow") <- sum(sapply(tmp, nrow))
   attr(tmp, "splitRowDistn") <- quantile(sapply(tmp, nrow), probs=seq(0, 1, by=0.0001))
   
   attr(tmp, "divBy") <- by
   tmp
}


