# not all test environments have Hadoop installed
TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
  TEST_HDFS <- FALSE

context("hexbin checks")

tmpDat <- data.frame(
  xx = rnorm(1000),
  yy = rnorm(1000),
  by = sample(letters, 1000, replace = TRUE),
  by2 = sample(letters[1:10], 1000, replace = TRUE))

data <- divide(tmpDat, by = "by", update = TRUE)

test_that("hexbin memory", {
  res <- drHexbin(data, xVar = "xx", yVar = "yy")
  compare <- hexbin(tmpDat$xx, tmpDat$yy)

  expect_true(all(res@cell == compare@cell))
  expect_true(all(res@count == compare@count))
  expect_true(sqrt(mean((res@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res@ycm - compare@ycm)^2)) < 1e-15)

  # plot(res)
  # plot(compare)
})

test_that("hexbin 'by' memory", {
  res <- drHexbin(data, xVar = "xx", yVar = "yy", by = "by2")
  res_a <- res[["by2=a"]]
  tmpDat2 <- subset(tmpDat, by2 == "a")
  compare <- hexbin(tmpDat2$xx, tmpDat2$yy,
    xbnds = summary(data)$xx$range,
    ybnds = summary(data)$yy$range)

  expect_true(all(res_a@cell == compare@cell))
  expect_true(all(res_a@count == compare@count))
  expect_true(sqrt(mean((res_a@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res_a@ycm - compare@ycm)^2)) < 1e-15)
})

path <- file.path(tempdir(), "hexbin_test")
unlink(path, recursive = TRUE)

datald <- convert(data, localDiskConn(path, autoYes = TRUE))

test_that("hexbin local disk", {
  res <- drHexbin(datald, xVar = "xx", yVar = "yy")
  compare <- hexbin(tmpDat$xx, tmpDat$yy)

  expect_true(all(res@cell == compare@cell))
  expect_true(all(res@count == compare@count))
  expect_true(sqrt(mean((res@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res@ycm - compare@ycm)^2)) < 1e-15)
})

test_that("hexbin 'by' local disk", {
  res <- drHexbin(datald, xVar = "xx", yVar = "yy", by = "by2")
  res_a <- res[["by2=a"]]
  tmpDat2 <- subset(tmpDat, by2 == "a")
  compare <- hexbin(tmpDat2$xx, tmpDat2$yy,
    xbnds = summary(data)$xx$range,
    ybnds = summary(data)$yy$range)

  expect_true(all(res_a@cell == compare@cell))
  expect_true(all(res_a@count == compare@count))
  expect_true(sqrt(mean((res_a@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res_a@ycm - compare@ycm)^2)) < 1e-15)
})


if(TEST_HDFS) {

library(Rhipe)
rhinit()

path <- "/tmp/hexbin_test"
try(rhdel(path), silent = TRUE)

datahd <- convert(data, hdfsConn(path, autoYes = TRUE))

test_that("hexbin HDFS", {
  res <- drHexbin(datahd, xVar = "xx", yVar = "yy")
  compare <- hexbin(tmpDat$xx, tmpDat$yy)

  expect_true(all(res@cell == compare@cell))
  expect_true(all(res@count == compare@count))
  expect_true(sqrt(mean((res@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res@ycm - compare@ycm)^2)) < 1e-15)
})

test_that("hexbin 'by' HDFS", {
  res <- drHexbin(datahd, xVar = "xx", yVar = "yy", by = "by2")
  res_a <- res[["by2=a"]]
  tmpDat2 <- subset(tmpDat, by2 == "a")
  compare <- hexbin(tmpDat2$xx, tmpDat2$yy,
    xbnds = summary(data)$xx$range,
    ybnds = summary(data)$yy$range)

  expect_true(all(res_a@cell == compare@cell))
  expect_true(all(res_a@count == compare@count))
  expect_true(sqrt(mean((res_a@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res_a@ycm - compare@ycm)^2)) < 1e-15)
})

}
