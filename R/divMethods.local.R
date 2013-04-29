#' `[` Extract Method for 'localDiv'
#' 
#' `[` extrac method for 'localDiv'
#' 
#' @param x 'localDiv' object
#' @param ind index value
#' 
#' @details We want subsets of 'localDiv' to still behave like 'localDiv' objects.
#' 
#' @author Ryan Hafen
#' 
#' @export
"[.localDiv" <- function(x, ind) {
   class(x) <- "list"
   if(!is.numeric(ind))
      ind <- which(names(x) %in% ind)

   res <- x[ind]
   tmp <- attributes(x)
   tmp$names <- tmp$names[ind]
   attributes(res) <- tmp
   class(res) <- c("localDiv", "list")
   res
}

#' @export
"$.localDiv" <- function(x, val) {
   if(val %in% c("vars", "totSize", "ndiv", "trans", "nrow", "splitRowDistn", "divBy"))
      return(attr(x, val))
}


divExample.localDiv <- function(data, trans=FALSE) {
   if(trans) {
      return(attributes(data)$trans(data[[1]]))
   } else {
      return(data[[1]])
   }
}

divExampleKey.localDiv <- function(data, trans=FALSE) {
   names(data)[1]
}



#' @export
getKeys.localDiv <- function(obj) {
   names(obj)
}

getDivType.localDiv <- function(x) {
   attr(x, "divBy")$type
}

getDivAttr.localDiv <- function(x) {
   attributes(x)
}

divApply.localDiv <- function(data, apply) {
   keys <- getKeys(data)

   lapply(seq_along(data), function(ii) {
      apply$applyFn(c(apply$args, list(data=data[[ii]])), key=keys[ii])
   })
}

divCombine.localDiv <- function(data, map, apply, combine) {
   keys <- sapply(map, function(x) x$key)
   uKeys <- unique(keys)

   reduce <- combine$reduce
   
   applyReduce <- function(x) {
      reduce.key <- x
      reduce.values <- lapply(map[keys==x], function(a) a$val)
      rr <- list()
      rhcollect <- function(k, v) {
         rr[[length(rr) + 1]] <<- list(k, v)
      }
      eval(as.expression(reduce$pre))
      eval(as.expression(reduce$reduce))
      eval(as.expression(reduce$post))
      unlist(rr, recursive=FALSE)
   }

   # mimic reduce
   lapply(uKeys, applyReduce)
}



