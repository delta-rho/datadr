# not all test environments have Hadoop installed
TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
  TEST_HDFS <- FALSE

# much of updateAttributes is already tested in kvMemory, kvLocalDisk, etc.
# but there are some special cases to test that aren't covered there

context("test updateAttributes")

test_that("date/time summaries", {
  iris2 <- iris
  st <- Sys.time()
  iris2$time <- st + 0:149
  iris2$date <- as.Date(st) + 0:149
  iris2$time[10] <- NA
  iris2$date[22] <- NA

  a <- divide(iris2, by = "Species", update = TRUE)
  as <- summary(a)
  as

  expect_equal(as$time$range[1],  st)
  expect_equal(as$time$range[2], (st + 149))
  expect_equal(as$date$range[1], as.Date(st))
  expect_equal(as$date$range[2], as.Date(st) + 149)
})

context("test update with cat NAs")

test_that("update with all NA subset", {
  iris2 <- iris
  iris2$let <- sample(letters, 150, replace = TRUE)
  iris2$let[iris2$Species == "setosa"] <- NA
  tmp <- divide(iris2, by = "Species")
  tmp <- updateAttributes(tmp)

  expect_true(summary(tmp)$let$nna == 50)

  letTab <- summary(tmp)$let$freqTable
  letTab <- letTab[order(letTab$value),]
  orig <- data.frame(xtabs(~ let, data = iris2))
  expect_true(all(orig$Freq == letTab$Freq))
})


context("test drAggregate")

set.seed(1234)
aa <- sample(letters, 100, replace=TRUE)
bb <- data.frame(ct=sample(1:10, 100, replace=TRUE), let=aa, byvar=sample(c("fac1", "fac2"), 100, replace=TRUE))

res1 <- data.frame(xtabs(~ let, data=bb))
res1 <- res1[order(res1$Freq, res1$let, decreasing = TRUE),]
res1$let <- as.character(res1$let)
res1 <- subset(res1, Freq > 0)
res2 <- data.frame(xtabs(ct ~ let, data=bb))
res2 <- res2[order(res2$Freq, res2$let, decreasing = TRUE),]
res2 <- subset(res2, Freq > 0)
res2$let <- as.character(res2$let)
res3 <- data.frame(xtabs(~ let + byvar, data=bb))
res3 <- res3[order(res3$Freq, res3$let, res3$byvar, decreasing = TRUE),]
res3$let <- as.character(res3$let)
res3$byvar <- as.character(res3$byvar)
res3 <- subset(res3, Freq > 0)
res4 <- data.frame(xtabs(ct ~ let + byvar, data=bb))
res4 <- res4[order(res4$Freq, res4$let, res4$byvar, decreasing = TRUE),]
res4$let <- as.character(res4$let)
res4$byvar <- as.character(res4$byvar)
res4 <- subset(res4, Freq > 0)

bbddf <- ddf(list(list(1, bb[1:25,]), list(1, bb[26:75,]), list(1, bb[76:100,])))

test_that("drAggregate results match", {

  drRes1 <- drAggregate(bbddf, ~ let)
  drRes2 <- drAggregate(bbddf, ct ~ let)
  drRes3 <- drAggregate(bbddf, ~ let + byvar)
  drRes4 <- drAggregate(bbddf, ct ~ let + byvar)

  drRes1 <- drRes1[order(drRes1$Freq, drRes1$let, decreasing = TRUE),]
  drRes2 <- drRes2[order(drRes2$Freq, drRes2$let, decreasing = TRUE),]
  drRes3 <- drRes3[order(drRes3$Freq, drRes3$let, drRes3$byvar, decreasing = TRUE),]
  drRes4 <- drRes4[order(drRes4$Freq, drRes4$let, drRes4$byvar, decreasing = TRUE),]

  expect_identical(drRes1, res1)
  expect_identical(drRes2, res2)
  expect_identical(drRes3, res3)
  expect_identical(drRes4, res4)

  # using "by"
  drAggregate(ct ~ let, by = "byvar", data = bbddf)

  drAggregate(ct ~ let + byvar, by = c("let", "byvar"), data = bbddf)
})


if(TEST_HDFS) {

library(Rhipe)
rhinit()

path <- "/tmp/aggregate_test"
try(rhdel(path), silent = TRUE)

bbddfhd <- convert(bbddf, hdfsConn(path, autoYes = TRUE))

test_that("drAggregate hdfs results match", {
  drRes4 <- drAggregate(bbddfhd, ct ~ let + byvar)
  drRes4 <- drRes4[order(drRes4$Freq, drRes4$let, drRes4$byvar, decreasing = TRUE),]
  expect_identical(drRes4, res4)
})

}
