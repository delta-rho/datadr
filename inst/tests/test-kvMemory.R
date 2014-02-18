# not all test environments have Hadoop installed
TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
   TEST_HDFS <- FALSE

## make some dummy data to test on
set.seed(1234)
iris2 <- iris
iris2$Sepal.Length[5:8] <- NA
iris2$fac <- sample(letters, 150, replace = TRUE)
data <- list()
for(i in 1:25) {
   kk <- paste(c("a", "b", "c"), i, sep="")
   data[c(length(data) + 1):(length(data) + 3)] <- list(
      list(kk[1], iris2[1:10,]),
      list(kk[2], iris2[11:110,]),
      list(kk[3], iris2[111:150,])
   )
}
dataDigest <- sapply(data, digest)
datadf <- data.frame(rbindlist(lapply(data, "[[", 2)))

############################################################################
############################################################################
context("in-memory ddo checks")

test_that("initialize and print ddo", {
   mdo <- ddo(data)
   mdo
})

test_that("update ddo - check attrs", {
   mdo <- ddo(data, update = TRUE)
   
   expect_true(length(mdo) == 75)
   
   totSize <- getAttribute(mdo, "totSize")
   expect_true(!is.na(totSize))
   
   splitSizeRange <- diff(range(splitSizeDistn(mdo)))
   expect_true(splitSizeRange > 4800)
   
   getKeyKeys <- sort(unlist(getKeys(mdo)))
   keys <- sort(sapply(data, "[[", 1))
   expect_true(all(getKeyKeys == keys))
})

mdo <- ddo(data)

test_that("extraction checks", {
   expect_true(digest(mdo[[1]]) %in% dataDigest, label = "single extraction by index")
   key <- data[[1]][[1]]
   expect_equivalent(mdo[[key]], data[[1]], label = "single extraction by key")
   
   expect_true(all(sapply(mdo[c(1, 3)], digest) %in% dataDigest), label = "multiple extraction by index")
   keys <- c(data[[1]][[1]], data[[10]][[1]])
   expect_equivalent(mdo[keys], list(data[[1]], data[[10]]), label = "multiple extraction by key")
   
   # make sure this still works after updating
   mdo <- updateAttributes(mdo)
   key <- data[[1]][[1]]
   expect_equivalent(mdo[[key]], data[[1]], label = "single extraction by key after update")
})

############################################################################
############################################################################
context("in-memory ddf checks")

test_that("initialize and print ddf", {
   mdf <- ddf(data, reset = TRUE)
   mdf
})

mdf <- ddf(data, update = TRUE)

test_that("update ddf - check attrs", {
   expect_true(nrow(mdf) == 3750)
   expect_true(all(names(mdf) == names(iris2)))
   
   mdfSumm <- summary(mdf)
   
   # summaries
   expect_equal(mdfSumm$Sepal.Width$nna, length(which(is.na(datadf$Sepal.Width))))
   expect_equal(mdfSumm$Sepal.Width$range[1], min(datadf$Sepal.Width))
   expect_equal(mdfSumm$Sepal.Width$range[2], max(datadf$Sepal.Width))
   expect_equal(mdfSumm$Sepal.Width$stats$mean, mean(datadf$Sepal.Width))
   expect_equal(mdfSumm$Sepal.Width$stats$var, var(datadf$Sepal.Width))
   
   # summaries with NA
   expect_equal(mdfSumm$Sepal.Length$range[1], min(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(mdfSumm$Sepal.Length$range[2], max(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(mdfSumm$Sepal.Length$stats$mean, mean(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(mdfSumm$Sepal.Length$stats$var, var(datadf$Sepal.Length, na.rm = TRUE))
   # expect_equal(mdfSumm$Sepal.Length$stats$skewness, skewness(datadf$Sepal.Length))
   # expect_equal(mdfSumm$Sepal.Length$stats$kurtosis, kurtosis(datadf$Sepal.Length, type = 2))
   
   mdfSumm
})

############################################################################
############################################################################
context("in-memory divide() checks")

test_that("conditioning division and bsv", {
   mdd <- divide(mdf, by = "Species", update = TRUE,
      bsvFn = function(x)
         list(meanSL = bsv(mean(x$Sepal.Length))))
   
   # do some additional checks here...
   expect_true(hasExtractableKV(mdd))
   
   keys <- sort(unlist(getKeys(mdd)))
   expect_true(keys[1] == "Species=setosa")
   
   # TODO: check print method output more closely
   mdd
})

test_that("random replicate division", {
   mdr <- divide(mdf, by = rrDiv(nrow=200), postTransFn = function(x) { x$vowel <- as.integer(x$fac %in% c("a", "e", "i", "o", "u")); x })
})

############################################################################
############################################################################
context("in-memory recombine() checks")

mdd <- divide(mdf, by = "Species", update = TRUE,
   bsvFn = function(x)
      list(meanSL = bsv(mean(x$Sepal.Length))))
mpw <- mean(mdd[[1]][[2]]$Petal.Width)

test_that("simple recombination", {
   res <- recombine(mdd, apply = function(v) mean(v$Petal.Width))
   
   expect_equal(res[[1]][[2]], mpw)
})

test_that("recombination with combRbind", {
   res <- recombine(mdd, apply = function(v) mean(v$Petal.Width), comb = combRbind())
   expect_equal(res$val[res$Species=="setosa"], mpw)
})

test_that("recombination with combDdo", {
   meanApply <- function(v) {
      data.frame(mpw = mean(v$Petal.Width), mpl = mean(v$Petal.Length))
   }
   
   res <- recombine(mdd, apply = meanApply, comb = combDdo())
   
   expect_true(inherits(res, "ddo"))
})

test_that("recombination with drGLM", {
   set.seed(1234)
   mdr <- divide(mdf, by = rrDiv(nrow = 200), postTransFn = function(x) { x$vowel <- as.integer(x$fac %in% c("a", "e", "i", "o", "u")); x })
   
   a <- recombine(mdr, 
      apply = drGLM(vowel ~ Petal.Length, 
         family = binomial()), 
      combine = combMeanCoef())
})

############################################################################
############################################################################
context("in-memory conversion checks")

test_that("to disk", {
   path <- file.path(tempdir(), "mdd_test_convert")
   unlink(path, recursive = TRUE)

   mddDisk <- convert(mdd, localDiskConn(path, autoYes = TRUE))
   expect_true(nrow(mddDisk) == 3750)
})

if(TEST_HDFS) {
   test_that("to HDFS", {
      rhdel("/tmp/mdd_test_convert")
      mdfHDFS <- convert(mdd, hdfsConn(loc="/tmp/mdd_test_convert", autoYes=TRUE))
      expect_true(nrow(mdfHDFS) == 3750)
   })
}

## clean up

unlink(file.path(tempdir(), "mdd_test_convert"), recursive = TRUE)

