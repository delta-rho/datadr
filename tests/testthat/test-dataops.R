# not all test environments have Hadoop installed
TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
  TEST_HDFS <- FALSE

context("data operations checks (memory)")

bySpecies <- divide(iris, by = "Species")

test_that("lapply", {
  lapplyRes <- drLapply(bySpecies, function(x) mean(x$Sepal.Width))
  expect_true(inherits(lapplyRes, "ddo"))
  resList <- as.list(lapplyRes)
  expect_true(is.list(resList))
  expect_equal(as.numeric(resList[[1]][[2]]), 3.428)
})

test_that("filter", {
  filterRes <- drFilter(bySpecies, function(v) mean(v$Sepal.Width) < 3)
  expect_true(length(filterRes) == 2)
  expect_true(all(unlist(getKeys(filterRes)) == c("Species=versicolor", "Species=virginica")))
  expect_true(is.data.frame(filterRes[[1]]$value))
})

test_that("sample", {
  set.seed(234)
  sampleRes <- drSample(bySpecies, fraction = 0.25)
  expect_true(length(sampleRes) == 1)
  expect_true(sampleRes[[1]][[1]] == "Species=virginica")
})

test_that("subset", {
  a <- divide(iris, by = "Species")
  tmp <- drSubset(a, Sepal.Length > 6, preTransFn = flatten)
  tmp <- tmp[order(tmp$Sepal.Length),]

  comp <- subset(iris, Sepal.Length > 6)
  comp <- comp[order(comp$Sepal.Length),]

  expect_true(all(comp$Sepal.Length == tmp$Sepal.Length))
})



context("data operations checks (local disk)")

path <- file.path(tempdir(), "dataops_byspecies")
unlink(path, recursive = TRUE)
lapply_path <- file.path(tempdir(), "dataops_byspecies_lapply")
unlink(lapply_path, recursive = TRUE)
filter_path <- file.path(tempdir(), "dataops_byspecies_filter")
unlink(filter_path, recursive = TRUE)
sample_path <- file.path(tempdir(), "dataops_byspecies_sample")
unlink(sample_path, recursive = TRUE)

bySpecies <- divide(iris, by = "Species", output = localDiskConn(path, autoYes = TRUE))

test_that("lapply", {
  lapplyRes <- drLapply(bySpecies, function(x) mean(x$Sepal.Width),
    output = localDiskConn(lapply_path, autoYes = TRUE))
  expect_true(inherits(lapplyRes, "ddo"))
  resList <- as.list(lapplyRes)
  expect_true(is.list(resList))
  expect_equal(as.numeric(resList[[1]][[2]]), 3.428)
})

test_that("filter", {
  filterRes <- drFilter(bySpecies, function(v) mean(v$Sepal.Width) < 3,
    output = localDiskConn(filter_path, autoYes = TRUE))
  filterRes <- updateAttributes(filterRes)
  expect_true(length(filterRes) == 2)
  expect_true(all(unlist(getKeys(filterRes)) == c("Species=versicolor", "Species=virginica")))
  expect_true(is.data.frame(filterRes[[1]]$value))
})

test_that("sample", {
  set.seed(234)
  sampleRes <- drSample(bySpecies, fraction = 0.25,
    output = localDiskConn(sample_path, autoYes = TRUE))
  expect_true(length(sampleRes) == 1)
  expect_true(sampleRes[[1]][[1]] == "Species=virginica")
})

test_that("subset", {
  bySpecies2 <- addTransform(bySpecies, flatten)
  tmp <- drSubset(bySpecies2, Sepal.Length > 6)
  tmp <- tmp[order(tmp$Sepal.Length),]

  comp <- subset(iris, Sepal.Length > 6)
  comp <- comp[order(comp$Sepal.Length),]

  expect_true(all(comp$Sepal.Length == tmp$Sepal.Length))
})




if(TEST_HDFS) {

context("data operations checks (HDFS)")

path <- file.path("/tmp", "dataops_byspecies")
try(rhdel(path), silent = TRUE)
lapply_path <- file.path("/tmp", "dataops_byspecies_lapply")
try(rhdel(lapply_path), silent = TRUE)
filter_path <- file.path("/tmp", "dataops_byspecies_filter")
try(rhdel(filter_path), silent = TRUE)
sample_path <- file.path("/tmp", "dataops_byspecies_sample")
try(rhdel(sample_path), silent = TRUE)

bySpecies <- divide(iris, by = "Species", output = hdfsConn(path, autoYes = TRUE))

test_that("lapply", {
  lapplyRes <- drLapply(bySpecies, function(x) mean(x$Sepal.Width),
    output = hdfsConn(lapply_path, autoYes = TRUE))
  expect_true(inherits(lapplyRes, "ddo"))
  resList <- as.list(lapplyRes)
  expect_true(is.list(resList))
  expect_equal(as.numeric(resList[[1]][[2]]), 3.428)
})

test_that("filter", {
  filterRes <- drFilter(bySpecies, function(v) mean(v$Sepal.Width) < 3,
    output = hdfsConn(filter_path, autoYes = TRUE))
  filterRes <- updateAttributes(filterRes)
  expect_true(length(filterRes) == 2)
  expect_true(all(sort(unlist(getKeys(filterRes))) == c("Species=versicolor", "Species=virginica")))
  expect_true(is.data.frame(filterRes[[1]]$value))
})

## Need to work on random seed in hdfsConn
# test_that("sample", {
#   set.seed(234)
#   sampleRes <- drSample(bySpecies, fraction = 0.25,
#     output = hdfsConn(sample_path, autoYes = TRUE))
#   expect_true(length(sampleRes) == 1)
#   expect_true(sampleRes[[1]][[1]] == "Species=virginica")
# })

test_that("subset", {
  bySpecies2 <- addTransform(bySpecies, flatten)
  tmp <- drSubset(bySpecies2, Sepal.Length > 6)
  tmp <- tmp[order(tmp$Sepal.Length),]

  comp <- subset(iris, Sepal.Length > 6)
  comp <- comp[order(comp$Sepal.Length),]

  expect_true(all(comp$Sepal.Length == tmp$Sepal.Length))
})

}

