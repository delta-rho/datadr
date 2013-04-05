#' Get Split Keys
#'
#' Get split keys for divided data
#'
#' @param obj data object of either class "localDiv" or "rhData"
#'
#' @author Ryan Hafen
#' 
#' @export
getKeys <- function(obj) {
   UseMethod("getKeys", obj)
}

#' Get Split Keys
#'
#' Get split keys for divided data
#'
#' @param obj data object of "rhData"
#'
#' @author Ryan Hafen
#' 
#' @export
getKeys.rhData <- function(obj) {
   keys <- NULL
   if(existsOnHDFS(obj$loc, "_rh_meta", "keys.Rdata"))
      rhload(paste(obj$loc, "/_rh_meta/keys.Rdata", sep=""))
   keys
}

#' Get Split Keys
#'
#' Get split keys for divided data
#'
#' @param obj data object class "localDiv"
#'
#' @author Ryan Hafen
#' 
#' @export
getKeys.localDiv <- function(obj) {
   names(obj)
}

