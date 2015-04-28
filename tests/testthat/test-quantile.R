TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
  TEST_HDFS <- FALSE

# set up data
path <- file.path(tempdir(), "ldd_test")
unlink(path, recursive=TRUE)
conn <- localDiskConn(path, nBins=3, reset=TRUE, autoYes=TRUE)

for(i in 1:25) {
  kk <- paste(c("a", "b", "c"), i, sep="")
  data <- list(
    list(kk[1], iris[1:10,]),
    list(kk[2], iris[11:110,]),
    list(kk[3], iris[111:150,])
  )
  addData(conn, data)
}

ldd <- ddf(conn, reset = TRUE)

# in-memory version of same data frame (for comparisons)
tmp <- list()
for(i in 1:25) {
  tmp[[i]] <- iris
}
tmp <- do.call(rbind, tmp)

############################################################################
############################################################################
context("all-data quantile checks")

test_that("fail when min/max not known", {
  # expect error
  expect_error(drQuantile(ldd, var = "Sepal.Length", tails = 0))
})

ldd <- ddf(conn, update = TRUE)

test_that("result matches quantile()", {
  ### compare drQuantile output to default quantile() method
  sq <- drQuantile(ldd, var = "Sepal.Length", tails = 0)
  qq <- quantile(tmp$Sepal.Length, probs = seq(0, 1, by = 0.005), type=1)

  # plot(sq$fval, sq$q)
  # plot(seq(0, 1, by=0.005), qq)

  expect_true(mean(abs(qq - sq$q)) < 0.0001)
})

test_that("tails check", {
  sls <- sort(tmp$Sepal.Length)
  sq <- drQuantile(ldd, var="Sepal.Length", tails = 200)
  expect_true(all(head(sls, 200) == head(sq$q, 200)))
})

test_that("by=TRUE", {
  sq2 <- drQuantile(ldd, var = "Sepal.Length", by = "Species", tails = 0)
  # true quantiles
  tmpd <- divide(tmp, by="Species")
  tmpd2 <- addTransform(tmpd, function(x)
    data.frame(fval = seq(0, 1, by = 0.005),
      q = quantile(x$Sepal.Length,
        probs = seq(0, 1, by = 0.005), type = 3)))
  tmp2 <- recombine(tmpd2, combRbind)

  # library(lattice)
  # xyplot(q ~ fval, groups = group, data = sq2, auto.key = TRUE, type = c("p", "g"))
  # xyplot(q ~ fval, groups = Species, data = tmp2, auto.key = TRUE, type = c("p", "g"))
  # plot(sq2$q - tmp2$q)

  expect_true(mean(abs(sq2$q - tmp2$q)) < 0.001)
})

test_that("map is cleaned up for mulitple blocks", {
  # set map_buff_size_bytes small so that mulitple blocks are sent through
  # a single map task -- this is more of a localDisk mapReduce test to make
  # sure that it cleans up the map each time
  # (the "by" argument gets updated but should be clean for each map call)
  drQuantile(ldd, var = "Sepal.Length", control = list(map_buff_size_bytes = 10))
})


test_that("varTransFn", {
  sq <- drQuantile(ldd, var = "Sepal.Length", by = "Species", tails = 0, varTransFn = function(x) log(x))

  tmpd <- divide(tmp, by="Species")
  tmpd2 <- addTransform(tmpd, function(x)
    data.frame(fval = seq(0, 1, by = 0.005),
      q = quantile(log(x$Sepal.Length),
        probs = seq(0, 1, by = 0.005), type = 3)))
  tmp2 <- recombine(tmpd2, combRbind)

  expect_true(mean(abs(sq$q - tmp2$q)) < 0.0001)
  # xyplot(q ~ fval | group, data = sq)
  # xyplot(q ~ fval | Species, data = tmp2)
})

test_that("preTransFn", {
  sq <- drQuantile(ldd, var = "Sepal.Length", by = "Species2", tails = 0, varTransFn = function(x) log(x), preTransFn = function(x) { x$Species2 <- paste(x$Species, "2"); x })

  tmpd <- divide(tmp, by="Species")
  tmp2 <- recombine(tmpd,
    apply = function(x)
      data.frame(fval = seq(0, 1, by = 0.005),
        q = quantile(log(x$Sepal.Length),
          probs = seq(0, 1, by = 0.005), type = 3)),
    combine = combRbind())

  expect_true(mean(abs(sq$q - tmp2$q)) < 0.0001)
  # xyplot(q ~ fval | group, data = sq)
  # xyplot(q ~ fval | Species, data = tmp2)
})

test_that("multiple conditioning", {
  tmp2 <- tmp
  tmp2$let <- c("a", "b", "c")
  ldd2 <- divide(tmp2, by = rrDiv(500), update = TRUE)
  res <- drQuantile(ldd2, var = "Sepal.Length", by = c("Species", "let"))

  # xyplot(q ~ fval | Species * let, data = res)
  res <- subset(res, Species == "setosa" & let == "a")
  qres <- quantile(subset(tmp2, let == "a" & Species == "setosa")$Sepal.Length, probs = res$fval, type = 3)
  expect_true(mean(abs(res$q - qres)) < 0.002)
})

if(TEST_HDFS) {

library(Rhipe)
rhinit()

path <- "/tmp/rhipeTest/quant1"
try(rhdel(path), silent = TRUE)

hdd <- convert(ldd, hdfsConn(path, autoYes = TRUE))

sq2 <- drQuantile(hdd, var = "Sepal.Length", by = "Species", tails = 0)
# true quantiles
tmpd <- divide(tmp, by="Species")
tmpd2 <- addTransform(tmpd, function(x)
  data.frame(fval = seq(0, 1, by = 0.005),
    q = quantile(x$Sepal.Length,
      probs = seq(0, 1, by = 0.005), type = 3)))
tmp2 <- recombine(tmpd2, combRbind)

expect_true(mean(abs(sq2$q - tmp2$q)) < 0.001)

}
