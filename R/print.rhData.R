#' Print an "rhData" or "rhDF" Object
#'
#' Print an "rhData" or "rhDF" object
#'
#' @param x object to be printed
#' 
#' @details Prints overview of attributes in objects of class "rhData", "rhDF", as well as "rhDiv"
#' 
#' @author Ryan Hafen
#' 
#' @export
print.rhData <- function(x) {
   
   # to get the name of the x:
   # http://jackman.stanford.edu/blog/?p=800
   
   updateAttrText <- "[empty] call updateAttributes(dat) to get this value"
   
   dataAttrList <- list(
      "hasKeys"=list(
         desc="have all the keys been read in and saved?",
         render=function(x) ifelse(x, "keys are available through getKeys(dat)", updateAttrText)),
      "loc"=list(
         desc="location on hdfs",
         render=identity),
      "type"=list(
         desc="type of data file",
         render=identity),
      "nfile"=list(
         desc="number of HDFS files",
         render=identity),
      "totSize"=list(
         desc="total size of data on HDFS",
         render=prettySize),
      "ndiv"=list(
         desc="number of key/value pairs",
         render=function(x) NArender(x, x, updateAttrText)),
      "splitSizeDistn"=list(
         desc="distribution of the size of key/value pairs",
         render=function(x) NArender(x, "use dat$splitSizeDistn to get distribution", updateAttrText)),
      "sourceData"=list(
         desc="the data from which this data was created",
         render=function(x) NArender(x, sourceDataText(x), "[empty] - no source job")),
      "sourceJobData"=list(
         desc="job info for the MapReduce job that created this data",
         render=function(x) NArender(x, "use dat$sourceJobData to get job info data", "[empty] - no source job")),
      "example"=list(
         desc="example splits of the data",
         render=function(x) NArender(x, "use divExample(dat) to get an example subset (value only) or dat$example to get example key/value pairs", updateAttrText))
   )
   
   ### now for rhDF-specific attributes
   
   dfAttrList <- list(
      "vars"=list(
         desc="variables in the data.frame",
         render=printVars),
      "trans"=list(
         desc="transformation function to convert to data.frame",
         render=printTrans),
      "badSchema"=list(
         desc="which subsets have a 'schema' that does not match the majority",
         render=function(x) "(not implemented)"),
      "nrow"=list(
         desc="number of rows in the data.frame",
         render=function(x) NArender(x, x, updateAttrText)),
      "splitRowDistn"=list(
         desc="distribution of the number of rows in each key/value pair",
         render=function(x) NArender(x, "use dat$splitRowDistn to get distribution", updateAttrText)),
      "summary"=list(
         desc="summary statistics for each variable",
         render=function(x) "(not implemented)")
   )
   
   objClass <- class(x)
   objClass <- objClass[grepl("^rh", objClass)]
   title <- paste(objClass, collapse=", ")
   title <- paste("\nAn object of class", title, "with the following attributes: \n")
   
   dataTab <- buildPrintTable(x, dataAttrList, "rhData")
   dfTab <- NULL
   if("rhDF" %in% objClass)
      dfTab <- buildPrintTable(x, dfAttrList, "rhDF")

   divAttrList <- list(
      "condDiv"=list(
         desc="Conditioning variable division",
         render=condDivText),
      "rrDiv"=list(
         desc="Random replicate divison",
         render=rrDivText)
   )
   
   ## text for rhDiv
   divTab <- NULL
   if("rhDiv" %in% objClass) {
      divTab <- sapply(1, function(i) {
         curName <- x$divBy$type
         paste(
            ifelse(i==1, "First-order", "Second-order"),
            " division:\n",
            "  Type: ", divAttrList[[curName]]$desc, "\n",
            "    ", divAttrList[[curName]]$render(x$divBy), "\n",
            sep=""
         )
      })
      divTab <- paste(divTab, collapse="\n")
   }
   
   cat(title, "\n", dataTab, "\n\n", dfTab, "\n\n", divTab, sep="")
}
