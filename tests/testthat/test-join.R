# not all test environments have Hadoop installed
TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
  TEST_HDFS <- FALSE

context("join checks")

bySpecies <- divide(iris, by = "Species")
# get independent lists of just SW and SL
sw <- drLapply(bySpecies, function(x) x$Sepal.Width)
sl <- drLapply(bySpecies, function(x) x$Sepal.Length)

test_that("memory join", {
  a1 <- drJoin(Sepal.Width = sw, Sepal.Length = sl, postTransFn = as.data.frame)

  expect_true(all(names(a1[[1]][[2]]) == c("Sepal.Width", "Sepal.Length")))

  expect_true(all(a1[["Species=setosa"]][[2]]$Sepal.Width == sw[["Species=setosa"]][[2]]))
})

test_that("local disk join", {
  swPath <- file.path(tempdir(), "sw")
  slPath <- file.path(tempdir(), "sl")
  unlink(swPath, recursive = TRUE)
  unlink(slPath, recursive = TRUE)

  swd <- convert(sw, localDiskConn(swPath, autoYes = TRUE))
  sld <- convert(sl, localDiskConn(slPath, autoYes = TRUE))

  a2 <- drJoin(Sepal.Width = swd, Sepal.Length = sld, postTransFn = as.data.frame)

  expect_true(all(names(a2[[1]][[2]]) == c("Sepal.Width", "Sepal.Length")))

  expect_true(all(a2[["Species=setosa"]][[2]]$Sepal.Width == sw[["Species=setosa"]][[2]]))
})

if(TEST_HDFS) {
  test_that("hdfs join", {
    try(rhdel("/tmp/rhipeTest/sw"), silent = TRUE)
    try(rhdel("/tmp/rhipeTest/sl"), silent = TRUE)
    try(rhdel("/tmp/rhipeTest/join"), silent = TRUE)

    swh <- convert(sw, hdfsConn("/tmp/rhipeTest/sw", autoYes = TRUE))
    slh <- convert(sl, hdfsConn("/tmp/rhipeTest/sl", autoYes = TRUE))

    a3 <- drJoin(Sepal.Width = swh, Sepal.Length = slh, postTransFn = as.data.frame,
      output = hdfsConn("/tmp/rhipeTest/join", autoYes = TRUE))

    expect_true(all(names(a3[[1]][[2]]) == c("Sepal.Width", "Sepal.Length")))

    expect_true(all(a3[["Species=setosa"]][[2]]$Sepal.Width == sw[["Species=setosa"]][[2]]))
  })
}


