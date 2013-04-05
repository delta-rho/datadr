#' Construct Split Keys
#'
#' Construct split keys for divided data
#'
#' @param obj an object of class 'rhDiv'
#' @param vals a data.frame of values to use to construct keys
#' 
#' @details A split key for divided data is a string that identifies the split.  For example, if a data set was conditioned on "var1" and "var2", then a split corresponding to var1="a" and var2="b" would have the key "var1=a|var2=b".  This function constructs a vector of keys based on an input data.frame specifying the key levels you want to extract data for.
#' 
#' @author Ryan Hafen
#' 
#' @export
makeKeys <- function(obj, vals) {
   if(!is.data.frame(vals))
      stop("'vals' must be a data.frame")

   type <- NULL
   if(inherits(obj, "rhDiv")) {
      type <- "rhDiv"
   } else if(inherits(obj, "localDiv")) {
      type <- "localDiv"
   }
   if(is.null(type))
      stop("'obj' must be an object of class 'rhDiv' or 'localDiv'")


   if(type=="rhDiv") {
      objAttr <- obj
   } else {
      objAttr <- attributes(obj)
   }

   if(objAttr$divBy$type != "condDiv")
      stop("This method only works for conditioning variable division")

   divNames <- objAttr$divBy$vars
   
   valNames <- names(vals)
   common <- intersect(divNames, valNames)
   needed <- setdiff(divNames, valNames)
   vals <- vals[,common, drop=FALSE]
   
   if(length(common) == 0)
      stop("There were not any common variables between conditioning variables in 'obj' and 'vals'")
      
   # if the supplied vals is missing a variable, need to fill it in
   if(length(common) != length(divNames)) {
      # we need to load in keys in this case
      keys <- getKeys(obj)

      n <- nrow(vals)
      vals2 <- data.frame(lapply(needed, function(x) {
         rep(".*", n)
      }))
      names(vals2) <- needed
      
      vals <- cbind(vals, vals2)[,divNames]
      
      tmp <- do.call(cbind, lapply(seq_along(divNames), function(x) paste(divNames[x], "=", vals[[x]], sep="")))
      rgx <- unique(apply(tmp, 1, function(x) paste(x, collapse="\\|")))
      res <- suppressWarnings(keys[which(grepl(rgx, keys))]) # make sure this is correct
   } else {
      tmp <- do.call(cbind, lapply(seq_along(divNames), function(x) paste(divNames[x], "=", vals[[x]], sep="")))
      res <- unique(apply(tmp, 1, function(x) paste(x, collapse="|")))
   }
   
   res
}

# irisSplit <- divide(iris, by="Species")
# makeKeys(irisSplit, data.frame(Species="virginica"))
