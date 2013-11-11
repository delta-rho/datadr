getDr <- function(name) get(name, envir=.datadrEnv)

.datadrEnv <- new.env()

# extractableKV: can you access k/v pairs by key?

# any object of class ddo must have these attributes
assign("requiredDdoAttrs", c("conn", "keys", "extractableKV", "totSize", "nDiv", "splitSizeDistn", "example", "bsvInfo", "div", "counters"), envir=.datadrEnv)

# any object of class ddf must have these attributes
assign("requiredDdfAttrs", c("vars", "transFn", "badSchema", "nRow", "splitRowDistn", "summary"), envir=.datadrEnv)

# attributes that have been implemented in updateAttributes
assign("implementedAttrVars", c("splitSizeDistn", "keys", "nDiv", "nRow", "splitRowDistn", "summary"), envir=.datadrEnv)

# print method helpers for ddo attributes
assign("ddoAttrPrintList", list(
   "keys"=list(
      desc="have all the keys been read in and saved?",
      render=function(x) ifelse(!all(is.na(x)) || length(x) > 1, "keys are available through getKeys(dat)", "[empty] call updateAttributes(dat) to get this value")),
   # "loc"=list(
   #    desc="location on hdfs",
   #    render=identity),
   # "type"=list(
   #    desc="type of data file",
   #    render=identity),
   # "nFile"=list(
   #    desc="number of HDFS files",
   #    render=identity),
   "totSize"=list(
      desc="total size of data on HDFS",
      render=function(x) NArender(x, prettySize(x), "[empty] will be added to updateAttributes")),
   "nDiv"=list(
      desc="number of key/value pairs",
      render=function(x) NArender(x, x, "[empty] call updateAttributes(dat) to get this value")),
   "splitSizeDistn"=list(
      desc="distribution of the size of key/value pairs",
      render=function(x) NArender(x, "use splitSizeDistn(dat) to get distribution", "[empty] call updateAttributes(dat) to get this value")),
   # "sourceData"=list(
   #    desc="the data from which this data was created",
   #    render=function(x) NArender(x, sourceDataText(x), "[empty] - no source job")),
   # "sourceJobData"=list(
   #    desc="job info for the MapReduce job that created this data",
   #    render=function(x) NArender(x, "use getAttribute(dat, \"sourceJobData\") to get job info data", "[empty] - no source job")),
   "example"=list(
      desc="example subsets of the data",
      render=function(x) NArender(x, "use kvExample(dat) to get an example subset", "[empty] call updateAttributes(dat) to get this value")),
   "bsvInfo"=list(
      desc="table of information about bsvs",
      render=function(x) NArender(x, "use bsvInfo(dat) to get BSV info", "[empty] no BSVs have been specified"))
), envir=.datadrEnv)

# print method helpers for ddf attributes
assign("ddfAttrPrintList", list(
   "vars"=list(
      desc="variables in the data frame",
      render=printVars),
   "transFn"=list(
      desc="transformation function to convert to data frame",
      render=printTrans),
   # "badSchema"=list(
   #    desc="which subsets have a 'schema' that does not match the majority",
   #    render=function(x) "(not implemented)"),
   "nRow"=list(
      desc="number of rows in the data frame",
      render=function(x) NArender(x, x, "[empty] call updateAttributes(dat) to get this value")),
   "splitRowDistn"=list(
      desc="distribution of the number of rows in each key/value pair",
      render=function(x) NArender(x, "use splitRowDistn(dat) to get distribution", "[empty] call updateAttributes(dat) to get this value")),
   "summary"=list(
      desc="summary statistics for each variable",
      render=function(x) NArender(x, "use summary(dat) to see summaries", "[empty] call updateAttributes(dat) to get this value"))
), envir=.datadrEnv)

# print method helpers for div attributes
assign("divAttrPrintList", list(
   "condDiv"=list(
      desc="Conditioning variable division",
      render=condDivText),
   "rrDiv"=list(
      desc="Random replicate divison",
      render=rrDivText)
), envir=.datadrEnv)

