#' Division-Agnostic Tabulation
#' 
#' Creates contingency tables from cross-classifying factors, with a formula interface similar to \code{xtabs}
#' 
#' @param formula a \code{\link{formula}} object with the cross-classifying variables (separated by +) on the right hand side (or an object which can be coerced to a formula). Interactions are not allowed. On the left hand side, one may optionally give a variable name in the data representing counts; in the latter case, the columns are interpreted as corresponding to the levels of a variable. This is useful if the data have already been tabulated.
#' @param data a "ddf" containing the variables in the formula \code{formula}
#' @param by an optional variable by which to split up tabulations (i.e. tabulate independently inside of each unique "by" variable value).  The only difference between specifying "by" and placing it in the right hand side of the formula is how the computation is done and how the result is returned.
#' @param transFn an optional function to apply to each subset prior to performing tabulation.  The output from this function should be a data frame containing variables with names that match that of the formula provided.
#' @param maxUnique the maximum number of unique combinations of variables to obtaion tabulations for.  This is meant to help against cases where a variable in the formula has a very large number of levels, to the point that it is not meaningful to tabulate and is too computationally burdonsome.  If \code{NULL}, it is ignored.  If a positive number, only the top and bottom \code{maxUnique} tabulations by frequency are kept.
#' @param params a named list of parameters external to the input data that are needed in the distributed computing (most should be taken care of automatically such that this is rarely necessary to specify)
#' @param control parameters specifying how the backend should handle things (most-likely parameters to \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}} and \code{\link{localDiskControl}}
#' 
#' @return a data frame of the tabulations.  When "by" is specified, it is a named list, with each element corresponding to a unique "by" value, containing a data frame of tabulations.
#' 
#' @author Ryan Hafen
#'
#' @seealso \code{\link{xtabs}}, \code{\link{updateAttributes}}
#'
#' @examples
#' drXtabs(Sepal.Length ~ Species, data = ddf(iris))
#' @export
drXtabs <- function(formula, data = data, by = NULL, transFn = NULL, maxUnique = NULL, params = NULL, control = NULL) {
   
   # TODO: check formula on a subset
   # if the number of levels of each factor is too insanely large
   # then warn or stop
   
   if(is.null(transFn))
      transFn <- identity
   
   # if formula does not contain the "by" variable, add it in
   if(!is.null(by))
      if(!by %in% labels(terms(formula)))
         formula <- eval(parse(text = paste("update(formula, ~ . + ", by, ")", sep = "")))
   
   map <- expression({
      # tabulate all, group, and then collect for each unique "by"
      res <- do.call(rbind, lapply(seq_along(map.values), function(i) {
         v <- kvApply(transFn, list(map.keys[[i]], map.values[[i]]))
         tabulateMap(formula, v)
      }))
      
      tmp <- xtabs(Freq ~ ., data = res)
      if(length(tmp) > 0) { # tabulation of all NAs yields empty table
         res <- as.data.frame(tmp, stringsAsFactors = FALSE)
         
         if(is.null(by)) {
            collect("global", res)
         } else {
            # go through for each "by"
            unique.by <- as.character(unique(res[,by]))
            lapply(unique.by, function(uby) {
               ind <- which(res[,by] == uby)
               if(length(ind) > 0)
                  collect(uby, res[ind,])
            })
         }
      }
   })
   
   reduce <- expression(
      pre = { res <- NULL }, 
      reduce = { res <- tabulateReduce(res, reduce.values, maxUnique) }, 
      post = { collect(reduce.key, res) }
   )

   globalVars <- drFindGlobals(transFn)
   globalVarList <- getGlobalVarList(globalVars, parent.frame())
   parList <- list(
      formula = formula,
      by = by,
      transFn = transFn,
      maxUnique = maxUnique
   )
   
   res <- mrExec(data,
      map = map,
      reduce = reduce,
      params = c(globalVarList, parList, params),
      control = control
   )
   
   res <- as.list(res)
   nms <- sapply(res, "[[", 1)
   res <- lapply(res, "[[", 2)
   names(res) <- nms
   if(length(res) == 1 && nms[1] == "global")
      res <- res[[1]]
   
   res
}


