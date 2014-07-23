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
- add `packages` argument to compute-inducing functions to allow manual
  specification of package dependencies required by user defined 
  transformations

