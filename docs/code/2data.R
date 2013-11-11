

# simple key-value pair example
list(1:5, rnorm(10))



# create by-species key-value pairs
irisKV <- list(
   list("setosa", subset(iris, Species=="setosa")[,1:2]),
   list("versicolor", subset(iris, Species=="versicolor")[,1:2]),
   list("virginica", subset(iris, Species=="virginica")[,1:2])
)
irisKV



# kvApply example operating on just value
meanSepalLength1 <- function(v)
   mean(v$Sepal.Length)
   
kvApply(meanSepalLength1, irisKV[[1]])



# kvApply example operating on key and value
meanSepalLength2 <- function(k, v)
   data.frame(species=k, mean=mean(v$Sepal.Length))
   
kvApply(meanSepalLength2, irisKV[[1]])



# create ddo object from irisKV
irisDdo <- ddo(irisKV)



irisDdo



# look at irisDdo keys
getKeys(irisDdo)



# look at an example key-value pair of irisDdo
kvExample(irisDdo)



# update irisDdo attributes
irisDdo <- updateAttributes(irisDdo)
irisDdo



par(mar=c(4.1, 4.1, 1, 0.2))
# plot distribution of the size of the key-value pairs
plot(splitSizeDistn(irisDdo))



# update at the time ddo() is called
irisDdo <- ddo(irisKV, update=TRUE)



irisDdo[["setosa"]]
irisDdo[[1]]



irisDdo[c("setosa", "virginica")]
irisDdo[1:2]



# create ddf object from irisKV
irisDdf <- ddf(irisKV, update=TRUE)
irisDdf



# look at irisDdf summary stats
summary(irisDdf)



nrow(irisDdf)
ncol(irisDdf)
names(irisDdf)



# initialize ddf from a data frame
irisDf <- ddf(iris, update=TRUE)



# example of some "less-structured" key-value pairs
people <- list(
   list("fred", 
      list(age=74, statesLived=c("NJ", "MA", "ND", "TX"))
   ),
   list("bob", 
      list(age=42, statesLived="NJ")
   )
)



# cast first value as data frame
as.data.frame(people[[1]][[2]])



# ddf with transFn
peopleDdf <- ddf(people, transFn=as.data.frame)



# ddf tries as.data.frame for transFn by default
peopleDdf <- ddf(people)



# get a ddf key-value pair with transFn applied
kvExample(peopleDdf, transform=TRUE)



# data is still stored unstructured (pre transFn)
kvExample(peopleDdf)


