############################################################################
############################################################################
context("find / apply global checks")

bySpecies <- divide(iris, by = "Species")

test_that("applyTransform works", {
  slMean <- addTransform(bySpecies, function(x) mean(x$Sepal.Length))
  expect_equal(as.numeric(slMean[["Species=setosa"]][[2]]), 5.006)
})

# expect_error(drQuantile(ldd, var = "Sepal.Length", tails = 0))

test_that("2-level functions work", {
  slMean <- addTransform(bySpecies, function(x) mean(x$Sepal.Length))
  slMean2 <- addTransform(slMean, function(x) data.frame(mean = x))

  expect_equal(slMean2[["Species=setosa"]][[2]]$mean, 5.006)
})

test_that("objects preserved after removed", {
  # checks nested functions, package detection, removing objects
  # and that objects are added even when NULL
  require(lattice)

  a <- 12; aa <- 2; aaa <- NULL; b <- 2
  gg <- function() {xyplot(1:10 ~ 1:10); if(is.null(aaa)) b}
  ff <- function(x) mean(x$Sepal.Length) + aa + a + gg()

  slFf <- addTransform(bySpecies, ff)

  expect_equal(as.numeric(kvExample(slFf)[[2]]), 21.006)

  # what if we change
  b <- 3
  expect_equal(as.numeric(kvExample(slFf)[[2]]), 21.006)

  rm(b)
  expect_equal(as.numeric(kvExample(slFf)[[2]]), 21.006)

  # should error out because b not found
  expect_error(slFf2 <- addTransform(bySpecies, ff))

  # do extractors work?
  expect_equal(as.numeric(kvExample(slFf)[[2]]), 21.006)
  rm(a)
  expect_equal(as.numeric(kvExample(slFf)[[2]]), 21.006)
  rm(ff)
  expect_equal(as.numeric(kvExample(slFf)[[2]]), 21.006)

  # are packages loaded?
  detach("package:lattice")
  kvExample(slFf)
})

test_that("Other methods with addTransform", {
  bySpeciesLogSL <- addTransform(bySpecies, function(x) {
    x$logSL <- log(x$Sepal.Length)
    x$slCut <- cut(x$Sepal.Length, seq(4, 8, by = 1))
    x
  })

  expect_error(updateAttributes(bySpeciesLogSL), "Cannot run")

  res <- drAggregate(logSL ~ slCut, data = bySpeciesLogSL)
  expect_equal(res$Freq[res$slCut == "(5,6]"], 97.617884823711108311)

  expect_error(drQuantile(bySpeciesLogSL, var = "logSL"), "Cannot run")
})



