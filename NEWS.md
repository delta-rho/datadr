Version 0.7.3.3
-------------------------------------------------------------------------------

FEATURES / CHANGES

- allow `by` argument in `drQuantile()` and `drAggregate()` to be a vector of column names
- add `output` ability to `drAggregate` for returning a ddf when `by` is specified
- change MapReduce logic to return ddf when the value is a data frame

BUG FIXES

- fix bug in finding file endings in `drRead.table()` for reading local files
- fix passing of `overwrite` parameter when using local `drRead.table()`
- fix bug in passing scientific numbers for rhipe_map_buff_size in `drRead.table()`
  with RHIPE / Hadoop backend
- fix bug when conditioning on more than one variable the key can get extra 
  spaces from numeric conditioning variables
- fix proper reduce setting for `drRead.table()` for HDFS
- fix indexing in divide map

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
- update `drGLM()` and `drBLB()` methods to work with new transformation 
  approach
- add `kvPair()` and classes for making dealing with key-value pairs a bit more 
  aesthetic
