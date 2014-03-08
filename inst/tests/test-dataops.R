context("data operations checks")

bySpecies <- divide(iris, by="Species")

test_that("lapply", {
   lapplyRes <- drLapply(bySpecies, function(x) mean(x$Sepal.Width))
   expect_true(inherits(lapplyRes, "ddo"))
   expect_true(inherits(lapplyRes, "ddo"))
   resList <- as.list(lapplyRes)
   expect_true(is.list(resList))
   expect_equal(as.numeric(resList[[1]][[2]]), 3.428)
})

test_that("join", {
   sw <- drLapply(bySpecies, function(x) x$Sepal.Width)
   sl <- drLapply(bySpecies, function(x) x$Sepal.Length)
   
   joinRes <- drJoin(Sepal.Width=sw, Sepal.Length=sl, postTransFn = as.data.frame)
   
   expect_true(all(names(joinRes[[1]][[2]]) == c("Sepal.Width", "Sepal.Length")))
})

test_that("filter", {
   filterRes <- drFilter(bySpecies, function(v) mean(v$Sepal.Width) < 3)
   expect_true(length(filterRes) == 2)
   expect_true(all(unlist(getKeys(filterRes)) == c("Species=versicolor", "Species=virginica")))
})

test_that("sample", {
   set.seed(234)
   sampleRes <- drSample(bySpecies, fraction = 0.25)
   expect_true(length(sampleRes) == 1)
   expect_true(sampleRes[[1]][[1]] == "Species=virginica")
})

