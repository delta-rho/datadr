#' Construct Between Subset Variable (BSV)
#'
#' Construct between subset variable (BSV)
#'
#' @param val a scalar character, numeric, or date
#' @param desc a character string describing the BSV
#'
#' @details
#' Should be called inside the \code{bsvFn} argument to \code{divide} used for constructing a BSV list for each subset of a division.
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{getBsvs}}, \code{\link{bsvInfo}}
#'
#' @examples
#' irisDdf <- ddf(iris)
#'
#' bsvFn <- function(dat) {
#'   list(
#'     meanSL = bsv(mean(dat$Sepal.Length), desc="mean sepal length"),
#'     meanPL = bsv(mean(dat$Petal.Length), desc="mean petal length")
#'   )
#' }
#'
#' # divide the data by species
#' bySpecies <- divide(irisDdf, by="Species", bsvFn=bsvFn)
#' # see BSV info attached to the result
#' bsvInfo(bySpecies)
#' # get BSVs for a specified subset of the division
#' getBsvs(bySpecies[[1]])
#'
#' @export
bsv <- function(val=NULL, desc="") {
  if(is.factor(val))
    val <- as.character(val)

  if(!(is.character(val) || is.numeric(val) || inherits(val, "Date") || inherits(val, "POSIXct")))
    val <- NA

  attr(val, "desc") <- desc
  val
}

bsv2df <- function(x) {
  # remove elements that have length greater than 1
  x <- x[sapply(x, length) == 1]
  data.frame(stripBsvAttr(x), stringsAsFactors=FALSE)
}

getBsvDesc <- function(x, bsvFn) {
  ex <- bsvFn(x)
  exnm <- names(ex)

  do.call(rbind, lapply(seq_along(ex), function(i) {
    desc <- attr(ex[[i]], "desc")
    if(is.null(desc)) desc <- ""
    data.frame(name = exnm[i], desc = desc, stringsAsFactors=FALSE)
  }))
}

# when we attach the list to each subset, we don't want to repeat storage of the "desc" attribute
stripBsvAttr <- function(obj) {
  for(i in seq_along(obj))
    attr(obj[[i]], "desc") <- NULL
  obj
}

validateBsvFn <- function(data, bsvFn, verbose=FALSE) {
  if(verbose)
    message("* Testing bsv function on a subset ... ", appendLF=FALSE)

  ex <- kvApply(bsvFn, data)

  if(!is.list(ex))
    stop("bsv function must return a list")

  exdf <- bsv2df(ex)
  if(ncol(exdf) < length(ex))
    warning("BSVs were removed when converting to data frame (have length greater than 1)")

  if(verbose)
    message("ok")
}

# function to add BSVs to a data set
# requires a complete read/write of the data
# addBSVs <- function()

# grab computed bsvs and put them into a table
# this will allow us to interact with the subsets
# if data.frame, check number of subsets to make sure
# it is reasonable - then store in _meta directory
# the other option is MongoDB
# NOTE: maybe this should be an attribute, and should be done in updateAttributes
# bsvTable <- function(obj, conn="data.frame") {
#   # mapreduce job to extract bsvs and splitvars in case of condDiv
# }




