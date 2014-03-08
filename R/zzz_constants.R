getDr <- function(name) get(name, envir = .datadrEnv)

.datadrEnv <- new.env()

# extractableKV: can you access k/v pairs by key?

# any object of class ddo must have these attributes
assign("requiredDdoAttrs", c("conn", "keys", "keyHashes", "extractableKV", "totStorageSize", "totObjectSize", "nDiv", "splitSizeDistn", "example", "bsvInfo", "div", "counters"), envir = .datadrEnv)

# any object of class ddf must have these attributes
assign("requiredDdfAttrs", c("vars", "transFn", "badSchema", "nRow", "splitRowDistn", "summary"), envir = .datadrEnv)

# attributes that have been implemented in updateAttributes
assign("implementedAttrVars", c("splitSizeDistn", "totObjectSize", "keys", "nDiv", "nRow", "splitRowDistn", "summary"), envir = .datadrEnv)

# print method helpers for ddo attributes
assign("ddoAttrPrintList", list(
   "keys" = list(
      desc = "have all the keys been read in and saved?",
      render = function(x) ifelse(!all(is.na(x)) || length(x) > 1, "keys are available through getKeys(dat)", "[empty] call updateAttributes(dat) to get this value")),
   "totStorageSize" = list(
      desc = "total size of data in storage",
      render = function(x) NArender(x, prettySize(x), "[empty] call updateAttributes(dat) to get this value")),
   "totObjectSize" = list(
      desc = "total size of data as R objects",
      render = function(x) NArender(x, prettySize(x), "[empty] call updateAttributes(dat) to get this value")),
   "nDiv" = list(
      desc = "number of key-value pairs",
      render = function(x) NArender(x, x, "[empty] call updateAttributes(dat) to get this value")),
   "splitSizeDistn" = list(
      desc = "distribution of the size of key-value pairs",
      render = function(x) NArender(x, "use splitSizeDistn(dat) to get distribution", "[empty] call updateAttributes(dat) to get this value")),
   "example" = list(
      desc = "example subsets of the data",
      render = function(x) NArender(x, "use kvExample(dat) to get an example subset", "[empty] call updateAttributes(dat) to get this value")),
   "bsvInfo" = list(
      desc = "table of information about bsvs",
      render = function(x) NArender(x, "use bsvInfo(dat) to get BSV info", "[empty] no BSVs have been specified"))
), envir = .datadrEnv)

# print method helpers for ddf attributes
assign("ddfAttrPrintList", list(
   "vars" = list(
      desc = "variables in the data frame",
      render = printVars),
   "transFn" = list(
      desc = "transformation function to convert to data frame",
      render = printTrans),
   # "badSchema" = list(
   #    desc = "which subsets have a 'schema' that does not match the majority",
   #    render = function(x) "(not implemented)"),
   "nRow" = list(
      desc = "number of rows in the data frame",
      render = function(x) NArender(x, x, "[empty] call updateAttributes(dat) to get this value")),
   "splitRowDistn" = list(
      desc = "distribution of the number of rows in each key-value pair",
      render = function(x) NArender(x, "use splitRowDistn(dat) to get distribution", "[empty] call updateAttributes(dat) to get this value")),
   "summary" = list(
      desc = "summary statistics for each variable",
      render = function(x) NArender(x, "use summary(dat) to see summaries", "[empty] call updateAttributes(dat) to get this value"))
), envir = .datadrEnv)

# print method helpers for div attributes
assign("divAttrPrintList", list(
   "condDiv" = list(
      desc = "Conditioning variable division",
      render = condDivText),
   "rrDiv" = list(
      desc = "Random replicate divison",
      render = rrDivText)
), envir = .datadrEnv)

