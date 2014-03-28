

irisDdf <- ddf(iris)
# divide irisDdf by species
bySpecies <- divide(irisDdf, by = "Species", update = TRUE)



bySpecies



# divide irisDdf by species using condDiv()
bySpecies <- divide(irisDdf, by = condDiv("Species"), update = TRUE)



# look at a subset of bySpecies
str(bySpecies[[1]])



# get the split variable (Species) for some subsets
getSplitVars(bySpecies[[1]])
getSplitVars(bySpecies[[2]])



# look at bySpecies keys
getKeys(bySpecies)



# divide iris data into random subsets of 10 rows per subset
set.seed(123)
byRandom <- divide(bySpecies, by = rrDiv(10), update = TRUE)



byRandom



par(mar = c(4.1, 4.1, 1, 0.2))
# plot distribution of the number of rows in each subset
plot(splitRowDistn(byRandom))



getKeys(byRandom)



# preTransFn to extract Sepal.Length from a value in a key-value pair
extractSepalLength <- function(v)
   v[, c("Sepal.Length", "Species")]
# test it on a subset to make sure it is doing what we want
head(kvApply(extractSepalLength, irisDdf[[1]]))
# apply division with preTransFn
bySpeciesSL <- divide(irisDdf, by = "Species", preTransFn = extractSepalLength)



# get summary statistics for Sepal.Length
summary(bySpecies)$Sepal.Length





# preTransFn to add a variable "slCut" of discretized Sepal.Length
sepalLengthCut <- function(v) {
   v$slCut <- cut(v$Sepal.Length, seq(0, 8, by = 1))
   v
}
# test it on a subset
slCutTest <- kvApply(sepalLengthCut, irisDdf[[1]])
head(slCutTest)
# divide on Species and slCut
bySpeciesSL <- divide(irisDdf, by = c("Species", "slCut"), 
   preTransFn = sepalLengthCut)



bySpeciesSL[[3]]





getSplitVars(bySpeciesSL[[3]])



# divide iris data by species, spilling to new key-value after 12 rows
bySpeciesSpill <- divide(irisDdf, by = "Species", spill = 12, update = TRUE)



# look at some subsets
bySpeciesSpill[[1]]
bySpeciesSpill[[5]]



# divide iris data by species, spill, and filter out subsets with <=5 rows
bySpeciesFilter <- divide(irisDdf, by = "Species", spill = 12,
   filter = function(v) nrow(v) > 5, update = TRUE)
bySpeciesFilter



recombine(bySpecies, apply = function(v) mean(v$Petal.Width))



recombine(bySpecies, apply = function(v) mean(v$Petal.Width), comb = combRbind())



meanApply <- function(v) {
   data.frame(mpw = mean(v$Petal.Width), mpl = mean(v$Petal.Length))
}
recombine(bySpecies, apply = meanApply, comb = combRbind())



recombine(bySpecies, apply = meanApply, comb = combDdo())



data(adult)
# turn adult into a ddf
adultDdf <- ddf(adult, update = TRUE)
adultDdf
#look at the names
names(adultDdf)



library(lattice)
edTable <- summary(adultDdf)$education$freqTable
edTable$value <- with(edTable, reorder(value, Freq, mean))
dotplot(value ~ Freq, data = edTable)



# make a preTransFn to group some education levels
edGroups <- function(v) {
   v$edGroup <- as.character(v$education)
   v$edGroup[v$edGroup %in% c("1st-4th", "5th-6th")] <- "Some-elementary"
   v$edGroup[v$edGroup %in% c("7th-8th", "9th")] <- "Some-middle"
   v$edGroup[v$edGroup %in% c("10th", "11th", "12th")] <- "Some-HS"
   v
}



# divide by edGroup and filter out "Preschool"
byEdGroup <- divide(adultDdf, by = "edGroup", 
   preTransFn = edGroups, 
   filterFn = function(x) x$edGroup[1] != "Preschool",
   update = TRUE)
byEdGroup



# tabulate number of people in each education group
edGroupTable <- recombine(byEdGroup, apply = nrow, combine = combRbind())
edGroupTable



# compute male/female ratio by education group
sexRatio <- recombine(byEdGroup, apply = function(x) {
   tab <- table(x$sex)
   data.frame(maleFemaleRatio = tab["Male"] / tab["Female"])
}, combine = combRbind())
sexRatio



# make dotplot of male/female ratio by education group
sexRatio$edGroup <- with(sexRatio, reorder(edGroup, maleFemaleRatio, mean))
dotplot(edGroup ~ maleFemaleRatio, data = sexRatio)





# fit a glm to the original adult data frame
rglm <- glm(incomebin ~ educationnum + hoursperweek + sex, data = adult, family = binomial())
summary(rglm)



rrAdult <- divide(adultDdf, by = rrDiv(500), update = TRUE,
   postTrans = function(x) 
      x[,c("incomebin", "educationnum", "hoursperweek", "sex")])



recombine(
   data = rrAdult, 
   apply = drGLM(incomebin ~ educationnum + hoursperweek + sex, 
      family = binomial()), 
   combine = combMeanCoef())



recombine(rrAdult,
   apply = drBLB(
      statistic = function(x, weights)
         coef(glm(incomebin ~ educationnum + hoursperweek + sex, 
            data = x, weights = weights, family = binomial())),
      metric = function(x)
         quantile(x, c(0.05, 0.95)),
      R = 100,
      n = nrow(rrAdult)
   ),
   combine = combMean()
)




