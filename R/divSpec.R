# used in divide to decide how to chunk up current data
getCuts <- function(x, ...)
   UseMethod("getCuts")

# make sure div specification works for data
validateDivSpec <- function(x, ...)
   UseMethod("validateDivSpec")
