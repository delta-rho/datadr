Version 0.7.3
-------------------------------------------------------------------------------

FEATURES / CHANGES

- add `addTransform()` method to specify transformations to be applied to 
  ddo/ddf objects with deferred evaluation (see 
  https://github.com/tesseradata/datadr/issues/24 for more information)
- revamp `drGetGlobals()` to properly traverse environments of user-defined 
  transformation functions and find all global variables and all package 
  dependencies
- refine printing of ddo/ddf objects (was getting too verbose)
- add `packages` argument to MapReduce-inducing functions to allow manual
  specification of package dependencies required by user defined 
  transformations
- add ability to set `options(defaultLocalDiskControl = ...)`, etc. so that you 
  do not always need to specify `control=` in all MapReduce-inducing operations
- add print method for key-value pairs to show things nicely, particularly
  when the object is a ddf, only show top rows of value
- add labels "key" and "value" to key-value pairs

