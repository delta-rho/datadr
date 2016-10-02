Version 0.8
----------------------------------------------------------------------

FEATURES / CHANGES

- Update references point to DeltaRho (0.8.6)
- Fix for compatibility with `data.table` (0.8.6)
- Add `control` option to `makeExtractable()` (0.8.5)
- Add several new documentation examples (0.8.4)
- Update `removeData()` method for local disk connections (0.8.4)
- Remove Spark back-end files (0.8.3)
- Updates to get CRAN-ready (0.8.3)
- Provide information about the key when an error occurs when applying a transformation (0.8.1)
- Improve documentation for a variety of functions, including the addition of examples (0.8.0)
- Add `drLM()` recombination method for fitting linear models (0.8.0)
- Update versioning scheme to x.y.z (0.8.0)

FIXES

- Update unit test thresholds to be upheld on systems without long double (0.8.5)
- Fig bug in variable preservation for multiple successive divides (0.8.3)
- Fix bug in printing summaries for in-memory ddfs with variables with 10k+ unique levels (0.8.2)
- Add workaround for current RHIPE bug with `combRbind()` (0.8.0)

Version 0.7.6
----------------------------------------------------------------------

FEATURES / CHANGES

- clarify and improve documentation for numerous functions
- add `combDdf()` recombination method
- add 'by' argument to drHexbin
- update logic of how `kvApply()` handles output
- update `drAggregate()` so first argument is data to be consistent
- several documentation cleanup updates
- add `kvPair()` method for specifying a key-value pair
- improve environment handling in local disk back end
- more friendly error messages when HDFS connection is empty
- many more HDFS unit tests
- add `drPersist()` method to make transformations persistent
- add overwrite parameter to convert methods
- add handling of character outputs to default to input connection for convenience in swapping out back ends
- add `to_ddf()` for converting dplyr grouped tbls to ddfs
- allow explicit specificaiton of ranges for `drQuantile()` and `drHexbin()`

FIXES

- improve error handling in `drJoin()` to validate that input data sources are ddo's
- improve logic for dealing with NULL values in key-value pairs
- fix loading of RHIPE and modifying RHIPE regex
- make keys in drRead.table more unique
- more meaningful error messages for drHexbin and drQuantile when dealing with transforms
- fix bug in addTransform dealing with new kvApply behavior
- fix to namespace rbindlist in MapReduce code
- fixes in how global variables are found in transformations
- fix drHexbin to work without needing to pass s4 object to MapReduce
- many improvements and fixes to HDFS back end
- clean up several files and fix some check NOTEs
- fix localDisk MapReduce to properly handle NULL reduce
- fix bug in `drRead.table()` not overwriting output for local disk case
- fix globals to not search in "imports" environments
- fix some bugs in `drQuantile()`
- improve error messages for kvApply with keys and values as inputs
- improve error message in local disk MapReduce when there is no data after map
- fix bug in `divide()` filtering on conditioning variables
- fix bug where a ddo could be mistaken for a ddf after running a MR job

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
  https://github.com/delta-rho/datadr/issues/24 for more information)
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
