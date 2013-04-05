#' Divide
#'
#' Divide data into subsets based on different criteria
#'
#' @param data data to divide
#' @param \ldots other arguments passed to the specific divide method
#'
#' @details See the following for specific details for different data objects:
#' \itemize{
#'    \item{\code{\link{divide.data.frame}}: for data.frames}
#'    \item{\code{\link{divide.rhDF}}: for RHIPE data.frames}
#' } 
#'
#' @author Ryan Hafen
#' 
#' @export
divide <- function(data, ...) {
   UseMethod("divide", data)
}


