library(digest)
library(data.table)

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

stripKVattrs <- function(x) {
   if(inherits(x, "kvPair")) {
      names(x) <- NULL
      class(x) <- "list"
      return(x)      
   } else {
      lapply(x, function(a) {
         names(a) <- NULL
         class(a) <- "list"
         a
      })
   }
}

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
   
   totStorageSize <- getAttribute(mdo, "totStorageSize")
   expect_true(!is.na(totStorageSize))
   
   totObjectSize <- getAttribute(mdo, "totObjectSize")
   expect_true(!is.na(totObjectSize))
   
   splitSizeRange <- diff(range(splitSizeDistn(mdo)))
   expect_true(splitSizeRange > 4800)
   
   getKeyKeys <- sort(unlist(getKeys(mdo)))
   keys <- sort(sapply(data, "[[", 1))
   expect_true(all(getKeyKeys == keys))
})

mdo <- ddo(data)

test_that("extraction checks", {
   expect_true(digest(stripKVattrs(mdo[[1]])) %in% dataDigest, 
      label = "single extraction by index")
   key <- data[[1]][[1]]
   expect_equivalent(stripKVattrs(mdo[[key]]), data[[1]], 
      label = "single extraction by key")
   
   expect_true(all(sapply(stripKVattrs(mdo[c(1, 3)]), digest) %in% dataDigest), 
      label = "multiple extraction by index")
   keys <- c(data[[1]][[1]], data[[10]][[1]])
   expect_equivalent(stripKVattrs(mdo[keys]), list(data[[1]], data[[10]]), 
      label = "multiple extraction by key")
   
   expect_equivalent(mdo[[1]], mdo[[digest(mdo[[1]][[1]])]], 
      label = "extraction by key hash")
   
   # check extraction order
   keys <- c(data[[1]][[1]], data[[8]][[1]], data[[27]][[1]])   
   idxs <- list(c(1, 2, 3), c(1, 3, 2), c(2, 1, 3), c(2, 3, 1), c(3, 1, 2), c(3, 2, 1))
   
   for(idx in idxs) {
      idxLab <- paste(idx, collapse = ",")
      expect_true(all(sapply(mdo[keys[idx]], "[[", 1) == keys[idx]),
         label = paste("extraction key matching order for", idxLab))
   }
   
   for(idx in idxs) {
      idxLab <- paste(idx, collapse = ",")
      keyHash <- sapply(keys[idx], digest)
      expect_true(all(sapply(mdo[keyHash], "[[", 1) == keys[idx]),
         label = paste("extraction hash matching order for", idxLab))
   }
   
   # make sure this still works after updating
   mdo <- updateAttributes(mdo)
   key <- data[[1]][[1]]
   expect_equivalent(stripKVattrs(mdo[[key]]), data[[1]], label = "single extraction by key after update")
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

test_that("data frame divide", {
   mdf <- divide(mdf, by = "Species")
   mdf2 <- divide(datadf, by = "Species")
   attributes(mdf) <- NULL
   attributes(mdf2) <- NULL
   expect_true(digest(mdf) == digest(mdf2))
})

test_that("conditioning division and bsv", {
   mdd <- divide(mdf, by = "Species", update = TRUE,
      bsvFn = function(x)
         list(meanSL = bsv(mean(x$Sepal.Length))))
   
   # do some additional checks here...
   expect_true(hasExtractableKV(mdd))
   
   keys <- sort(unlist(getKeys(mdd)))
   expect_true(keys[1] == "Species=setosa")
   
   mdd
})

test_that("division with addTransform", {
   a <- 3
   mdf2 <- addTransform(mdf, function(x) {
      x$Petal.Width <- x$Petal.Width + a
      x
   })
   rm(a)
   mdd2 <- divide(mdf2, by = "Species")
   
   expect_true(min(mdd2[["Species=virginica"]][[2]]$Petal.Width) == 4.4)
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
   expect_equal(as.numeric(res[[1]][[2]]), mpw)
})

test_that("recombine with addTransform", {
   a <- 3
   mddMpw <- addTransform(mdd, function(v) mean(v$Petal.Width) + a)
   rm(a)
   res <- recombine(mddMpw, combRbind)
   
   expect_true(res$val[res$Species == "setosa"] == 3.246)
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
   
   # FIX
   # a <- recombine(mdr, 
   #    apply = drGLM(vowel ~ Petal.Length, 
   #       family = binomial()), 
   #    combine = combMeanCoef())
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

