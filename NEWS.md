Version 0.7.5.1
----------------------------------------------------------------------

- improve error messages for kvApply with keys and values as inputs
- fix some bugs in `drQuantile()`
- allow explicit specificaiton of ranges for `drQuantile()` and `drHexbin()`
- improve error message in local disk MapReduce when there is no data after map

Version 0.7.5
----------------------------------------------------------------------

- reintroduce SparkR support, leveraging several updates to SparkR that allow
  for loading / persisting RDDs on disk, lookup by key, etc.
- general code formatting changes
- change `_rh_meta` to `_meta`
- remove strict dependency on parallel package
- several small changes to help R CMD check

Version 0.7.4
----------------------------------------------------------------------

FEATURES / CHANGES

- allow `by` argument in `drQuantile()` and `drAggregate()` to be a vector of column names
- add `output` ability to `drAggregate` for returning a ddf when `by` is specified
- change MapReduce logic to return ddf when the value is a data frame
- add faster data frame specific divide method for conditioning division

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
