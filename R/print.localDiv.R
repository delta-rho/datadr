#' Print a "localDiv" Object
#'
#' Print a "localDiv" object
#'
#' @param x object to be printed
#' 
#' @details Prints overview of attributes in objects of class "localDiv"
#' 
#' @author Ryan Hafen
#' 
#' @export
print.localDiv <- function(x) {

   # "hasKeys"=list(
   #    desc="have all the keys been read in and saved?",
   #    render=function(x) ifelse(x, "keys are available through getKeys(dat)", updateAttrText)),
   
   dataAttrList <- list(
      "vars"=list(
         desc="variables in the data.frame",
         render=printVars),
      "ndiv"=list(
         desc="number of splits",
         render=identity),
      "nrow"=list(
         desc="number of rows in the data.frame",
         render=identity),
      "totSize"=list(
         desc="total size of data",
         render=prettySize),
      "splitRowDistn"=list(
         desc="distribution of the number of rows in each key/value pair",
         render=function(x) "use dat$splitRowDistn to get distribution"),
      "trans"=list(
         desc="transformation function to convert to data.frame",
         render=printTrans),
      "summary"=list(
         desc="summary statistics for each variable",
         render=function(x) "(not implemented)"),
      "example"=list(
         desc="example splits of the data",
         render=function(x) "use divExample(dat) or dat[[i]] to get example subset(s)")
   )
   
   objClass <- "localDiv"
   title <- paste(objClass, collapse=", ")
   title <- paste("\nAn object of class", title, "with the following attributes: \n")
   
   dataTab <- buildPrintTable(attributes(x), dataAttrList, "localDiv")
   
   divAttrList <- list(
      "condDiv"=list(
         desc="Conditioning variable division",
         render=condDivText),
      "rrDiv"=list(
         desc="Random replicate divison",
         render=rrDivText)
   )
   
   ## text for div
   divTab <- sapply(1, function(i) {
      curName <- attributes(x)$divBy$type
      paste(
         ifelse(i==1, "First-order", "Second-order"),
         " division:\n",
         "  Type: ", divAttrList[[curName]]$desc, "\n",
         "    ", divAttrList[[curName]]$render(attributes(x)$divBy), "\n",
         sep=""
      )
   })
   divTab <- paste(divTab, collapse="\n")
 
   cat(title, "\n", dataTab, "\n\n", divTab, sep="")
}
