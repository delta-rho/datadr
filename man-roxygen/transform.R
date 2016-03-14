# Create a distributed data frame using the iris data set, backed by the
# kvMemory (in memory) connection
bySpecies <- divide(iris, by = "Species")
bySpecies
# Note a tranformation is not present in the attributes
names(attributes(bySpecies))
## A transform that operates only on values of the key-value pairs
##----------------------------------------------------------------
# Create a function that will calculate the mean of each variable in
# in a subset. The calls to 'as.data.frame()' and 't()' convert the
# vector output of 'apply()' into a data.frame with a single row
colMean <- function(x) as.data.frame(t(apply(x, 2, mean)))
# Test on a subset
colMean(bySpecies[[1]][[2]])
# Add a tranformation that will calculate the mean of each variable
bySpeciesTransformed <- addTransform(bySpecies, colMean)
# Note how 'before transformation' appears to describe the values of
# several of the attributes
bySpeciesTransformed
# Note the addition of the transformation to the attributes
names(attributes(bySpeciesTransformed))
# We can see the result of the transformation by looking at one of
# the subsets:
bySpeciesTransformed[[1]]
# The transformation is automatically applied when calling any data
# operation.  For example, if can call 'recombine()' with 'combRbind'
# we will get a data frame of the column means for each subset:
varMeans <- recombine(bySpeciesTransformed, combine = combRbind)
varMeans
## A transform that operates on both keys and values
##---------------------------------------------------------
# We can also create a transformation that uses both the keys and values
# It will select the first row of the value, and append '-firstRow' to
# the key
aTransform <- function(key, val) {
  newKey <- paste(key, "firstRow", sep = "-")
  newVal <- val[1,]
  kvPair(newKey, newVal)
}
# Apply the transformation
recombine(addTransform(bySpecies, aTransform))
