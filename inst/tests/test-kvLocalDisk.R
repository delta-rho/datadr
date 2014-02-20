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
context("local disk connection checks")

path <- file.path(tempdir(), "ldd_test")
unlink(path, recursive=TRUE)

test_that("initialize", {
   conn <- localDiskConn(path, autoYes=TRUE)   
})

test_that("add data", {
   conn <- localDiskConn(path)
   addData(conn, data)
   
   nFiles <- length(list.files(file.path(path), pattern = "\\.Rdata$"))
   expect_equal(nFiles, 75, label = "files written correctly")
   
   load(file.path(path, paste(conn$fileHashFn(data[[1]][[1]], conn))))
   expect_equivalent(data[[1]][[1]], obj[[1]][[1]], label = "keys written correctly")
   expect_equivalent(data[[1]][[2]], obj[[1]][[2]], label = "values written correctly")
})

# TODO: add test checking addition of data to already-existing k/v

unlink(path, recursive=TRUE)

############################################################################
############################################################################
context("local disk connection checks with nBins")

path <- file.path(tempdir(), "ldd_test_nbin")
unlink(path, recursive=TRUE)

test_that("add data", {
   conn <- localDiskConn(path, nBins = 3, autoYes = TRUE)
   addData(conn, data)
   
   nFiles <- length(list.files(file.path(path), pattern = "[^_meta]"))
   expect_equal(nFiles, 3, label = "files put into bins correctly")
   
   load(file.path(path, paste(conn$fileHashFn(data[[1]][[1]], conn))))
   expect_equivalent(data[[1]][[1]], obj[[1]][[1]], label = "keys written correctly")
   expect_equivalent(data[[1]][[2]], obj[[1]][[2]], label = "values written correctly")
})

unlink(path, recursive=TRUE)

############################################################################
############################################################################
context("local disk connection checks with custom hash function")

path <- file.path(tempdir(), "ldd_test_filehash")
unlink(path, recursive=TRUE)

test_that("add data", {
   myFileHash <- function(keys, conn) {
      paste(keys, ".Rdata", sep="")
   }
   conn <- localDiskConn(path, fileHashFn = myFileHash, autoYes = TRUE)
   addData(conn, data)
   
   fileChar <- substr(list.files(file.path(path), pattern = "[^_meta]"), 1, 1)
   expect_true(all(sort(unique(fileChar)) == c("a", "b", "c")), label = "files put into bins correctly")
   
   load(file.path(path, paste(conn$fileHashFn(data[[1]][[1]], conn))))
   expect_equivalent(data[[1]][[1]], obj[[1]][[1]], label = "keys written correctly")
   expect_equivalent(data[[1]][[2]], obj[[1]][[2]], label = "values written correctly")
})

unlink(path, recursive=TRUE)

############################################################################
############################################################################
context("local disk ddo checks")

path <- file.path(tempdir(), "ldd_test")
unlink(path, recursive=TRUE)
conn <- localDiskConn(path, autoYes=TRUE)   
addData(conn, data)

test_that("initialize and print ddo", {
   ldo <- ddo(conn)
   ldo
})

test_that("update ddo - check attrs", {
   ldo <- ddo(conn, update = TRUE)
   
   expect_true(length(ldo) == 75)
   
   totSize <- getAttribute(ldo, "totSize")
   expect_true(!is.na(totSize))
   
   splitSizeRange <- diff(range(splitSizeDistn(ldo)))
   expect_true(splitSizeRange > 4800)
   
   getKeyKeys <- sort(unlist(getKeys(ldo)))
   keys <- sort(sapply(data, "[[", 1))
   expect_true(all(getKeyKeys == keys))
})

# TODO: test resetting the connection

ldo <- ddo(conn)

test_that("extraction checks", {
   expect_true(digest(ldo[[1]]) %in% dataDigest, label = "single extraction by index")
   key <- data[[1]][[1]]
   expect_equivalent(ldo[[key]], data[[1]], label = "single extraction by key")
   
   expect_true(all(sapply(ldo[c(1, 3)], digest) %in% dataDigest), label = "multiple extraction by index")
   keys <- c(data[[1]][[1]], data[[10]][[1]])
   expect_equivalent(ldo[keys], list(data[[1]], data[[10]]), label = "multiple extraction by key")
   
   expect_equivalent(ldo[[1]], ldo[[digest(ldo[[1]][[1]])]], label = "extraction by key hash")
})

pathBins <- file.path(tempdir(), "ldd_testBins")
unlink(pathBins, recursive=TRUE)
connBins <- localDiskConn(pathBins, autoYes=TRUE)   
addData(connBins, data)
ldoBins <- ddo(connBins)

test_that("extraction checks with nbins", {
   expect_true(digest(ldoBins[[1]]) %in% dataDigest, label = "single extraction by index")
   key <- data[[1]][[1]]
   expect_equivalent(ldoBins[[key]], data[[1]], label = "single extraction by key")

   expect_true(all(sapply(ldoBins[c(1, 3)], digest) %in% dataDigest), label = "multiple extraction by index")
   keys <- c(data[[1]][[1]], data[[10]][[1]])
   expect_equivalent(ldoBins[keys], list(data[[1]], data[[10]]), label = "multiple extraction by key")
})

############################################################################
############################################################################
context("local disk ddf checks")

test_that("initialize and print ddf", {
   ldf <- ddf(conn, reset = TRUE)
   ldf
})

ldf <- ddf(conn, reset = TRUE)

test_that("update ddf - check attrs", {
   ldf <- ddf(conn, update = TRUE)
   
   expect_true(nrow(ldf) == 3750)
   expect_true(all(names(ldf) == names(iris2)))
   
   ldfSumm <- summary(ldf)
   
   # summaries
   expect_equal(ldfSumm$Sepal.Width$nna, length(which(is.na(datadf$Sepal.Width))))
   expect_equal(ldfSumm$Sepal.Width$range[1], min(datadf$Sepal.Width))
   expect_equal(ldfSumm$Sepal.Width$range[2], max(datadf$Sepal.Width))
   expect_equal(ldfSumm$Sepal.Width$stats$mean, mean(datadf$Sepal.Width))
   expect_equal(ldfSumm$Sepal.Width$stats$var, var(datadf$Sepal.Width))
   
   # summaries with NA
   expect_equal(ldfSumm$Sepal.Length$range[1], min(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(ldfSumm$Sepal.Length$range[2], max(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(ldfSumm$Sepal.Length$stats$mean, mean(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(ldfSumm$Sepal.Length$stats$var, var(datadf$Sepal.Length, na.rm = TRUE))
   # expect_equal(ldfSumm$Sepal.Length$stats$skewness, skewness(datadf$Sepal.Length))
   # expect_equal(ldfSumm$Sepal.Length$stats$kurtosis, kurtosis(datadf$Sepal.Length, type = 2))
   
   ldfSumm
})

############################################################################
############################################################################
context("local disk parallel check")

# test_that("update in parallel and check", {
#    require(parallel)
#    ldf <- ddf(conn, reset = TRUE)
#    cl <- makeCluster(2)
#    ldf <- updateAttributes(ldf, control=localDiskControl(cluster=cl))
#    stopCluster(cl)
#    
#    expect_true(nrow(ldf) == 3750)
#    expect_true(all(names(ldf) == names(iris2)))
#    
#    ldfSumm <- summary(ldf)
#    
#    # summaries
#    expect_equal(ldfSumm$Sepal.Width$nna, length(which(is.na(datadf$Sepal.Width))))
#    expect_equal(ldfSumm$Sepal.Width$range[1], min(datadf$Sepal.Width))
#    expect_equal(ldfSumm$Sepal.Width$range[2], max(datadf$Sepal.Width))
#    expect_equal(ldfSumm$Sepal.Width$stats$mean, mean(datadf$Sepal.Width))
#    expect_equal(ldfSumm$Sepal.Width$stats$var, var(datadf$Sepal.Width))
#    
#    # summaries with NA
#    expect_equal(ldfSumm$Sepal.Length$range[1], min(datadf$Sepal.Length, na.rm = TRUE))
#    expect_equal(ldfSumm$Sepal.Length$range[2], max(datadf$Sepal.Length, na.rm = TRUE))
#    expect_equal(ldfSumm$Sepal.Length$stats$mean, mean(datadf$Sepal.Length, na.rm = TRUE))
#    expect_equal(ldfSumm$Sepal.Length$stats$var, var(datadf$Sepal.Length, na.rm = TRUE))
#    # expect_equal(ldfSumm$Sepal.Length$stats$skewness, skewness(datadf$Sepal.Length))
#    # expect_equal(ldfSumm$Sepal.Length$stats$kurtosis, kurtosis(datadf$Sepal.Length, type = 2))
#    
#    ldfSumm
# })

############################################################################
############################################################################
context("local disk divide() checks")

test_that("conditioning division and bsv", {
   path2 <- file.path(tempdir(), "ldd_test_div")
   unlink(path2, recursive=TRUE)
   
   ldd <- divide(ldf, by = "Species", output = localDiskConn(path2, autoYes = TRUE), update = TRUE,
      bsvFn = function(x)
         list(meanSL = bsv(mean(x$Sepal.Length))))
   
   # do some additional checks here...
   expect_true(hasExtractableKV(ldd))
   
   keys <- sort(unlist(getKeys(ldd)))
   expect_true(keys[1] == "Species=setosa")
   
   # TODO: check print method output more closely
   ldd
})

test_that("random replicate division", {
   path2 <- file.path(tempdir(), "ldd_test_rrdiv")
   unlink(path2, recursive=TRUE)
   
   ldf <- ddf(conn)
   ldr <- divide(ldf, by = rrDiv(nrow=200), output = localDiskConn(path2, autoYes = TRUE), postTransFn = function(x) { x$vowel <- as.integer(x$fac %in% c("a", "e", "i", "o", "u")); x })
})

############################################################################
############################################################################
context("local disk recombine() checks")

ldd <- ddf(localDiskConn(file.path(tempdir(), "ldd_test_div")))
mpw <- mean(ldd[[1]][[2]]$Petal.Width)

test_that("simple recombination", {
   res <- recombine(ldd, apply = function(v) mean(v$Petal.Width))
   
   expect_equal(res[[1]][[2]], mpw)
})

test_that("recombination with combRbind", {
   res <- recombine(ldd, apply = function(v) mean(v$Petal.Width), comb = combRbind())
   expect_equal(res$val[res$Species=="setosa"], mpw)
})

test_that("recombination with combDdo", {
   meanApply <- function(v) {
      data.frame(mpw=mean(v$Petal.Width), mpl=mean(v$Petal.Length))
   }
   
   res <- recombine(ldd, apply=meanApply, comb=combDdo())
   
   expect_true(inherits(res, "ddo"))
})

test_that("recombination with drGLM", {
   path2 <- file.path(tempdir(), "ldd_test_drglm")
   
   set.seed(1234)
   ldf <- ddf(conn)
   ldr <- divide(ldf, by = rrDiv(nrow=200), output = localDiskConn(path2, autoYes = TRUE), postTransFn = function(x) { x$vowel <- as.integer(x$fac %in% c("a", "e", "i", "o", "u")); x })
   
   a <- recombine(ldr, 
      apply = drGLM(vowel ~ Petal.Length, 
         family = binomial()), 
      combine = combMeanCoef())
})

############################################################################
############################################################################
context("local disk conversion checks")

test_that("to memory", {
   lddMem <- convert(ldd, NULL)
   expect_true(nrow(lddMem) == 3750)
})

if(TEST_HDFS) {
   test_that("to HDFS", {
      rhdel("/tmp/ldd_test_convert")
      ldfHDFS <- convert(ldd, hdfsConn(loc="/tmp/ldd_test_convert", autoYes=TRUE))
      expect_true(nrow(ldfHDFS) == 3750)
   })
}

## clean up

unlink(file.path(tempdir(), "ldd_test"), recursive = TRUE)
unlink(file.path(tempdir(), "ldd_test_div"), recursive = TRUE)
unlink(file.path(tempdir(), "ldd_test_drglm"), recursive = TRUE)
unlink(file.path(tempdir(), "ldd_test_rrdiv"), recursive = TRUE)
unlink(file.path(tempdir(), "ldd_testBins"), recursive = TRUE)




