# not all test environments have Hadoop installed
TEST_HDFS <- Sys.getenv("DATADR_TEST_HDFS")
if(TEST_HDFS == "")
   TEST_HDFS <- FALSE

if(TEST_HDFS) {

library(Rhipe)
rhinit()

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
context("hdfs connection checks")

path <- file.path("/tmp", "hdd_test")
try(rhdel(path), silent = TRUE)

test_that("initialize", {
   conn <- hdfsConn(path, autoYes=TRUE)   
})

test_that("add data", {
   conn <- hdfsConn(path)
   addData(conn, data)
   
   paths <- rhls(path, recurse = TRUE)$file
   a <- rhread(paths, max = 1)
   
   expect_true(digest(a[[1]]) %in% dataDigest)
})

# TODO: add test checking addition of data to already-existing k/v

############################################################################
############################################################################
context("hdfs ddo checks")

path <- file.path("/tmp", "hdd_test")
conn <- hdfsConn(path, autoYes=TRUE)   

test_that("initialize and print ddo", {
   hdo <- ddo(conn)
   hdo
})

test_that("update ddo - check attrs", {
   hdo <- ddo(conn, update = TRUE)
   
   expect_true(length(hdo) == 75)
   
   totSize <- getAttribute(hdo, "totSize")
   expect_true(!is.na(totSize))
   
   splitSizeRange <- diff(range(splitSizeDistn(hdo)))
   expect_true(splitSizeRange > 4800)
   
   getKeyKeys <- sort(unlist(getKeys(hdo)))
   keys <- sort(sapply(data, "[[", 1))
   expect_true(all(getKeyKeys == keys))
})

# TODO: test resetting the connection

hdo <- ddo(conn)

test_that("extractableKV", {
   expect_false(hasExtractableKV(hdo))
})

test_that("extraction when not possible", {
   expect_error(hdo[["a1"]])
})

test_that("extraction by index", {
   hdo[[1]]
})

test_that("makeExtractable", {
   hdo <- makeExtractable(hdo)
})

hdo <- ddo(conn)

test_that("extraction checks", {
   expect_true(digest(hdo[[1]]) %in% dataDigest, label = "single extraction by index")
   key <- data[[1]][[1]]
   expect_equivalent(hdo[[key]], data[[1]], label = "single extraction by key")
   
   expect_true(all(sapply(hdo[c(1, 3)], digest) %in% dataDigest), label = "multiple extraction by index")
   keys <- c(data[[1]][[1]], data[[10]][[1]])
   expect_equivalent(hdo[keys], list(data[[1]], data[[10]]), label = "multiple extraction by key")
})

############################################################################
############################################################################
context("hdfs ddf checks")

test_that("initialize and print ddf", {
   hdf <- ddf(conn)
   hdf
})

hdf <- ddf(conn)

test_that("update ddf - check attrs", {
   hdf <- ddf(conn, update = TRUE)
   
   expect_true(nrow(hdf) == 3750)
   expect_true(all(names(hdf) == names(iris2)))
   
   hdfSumm <- summary(hdf)
   
   # summaries
   expect_equal(hdfSumm$Sepal.Width$nna, length(which(is.na(datadf$Sepal.Width))))
   expect_equal(hdfSumm$Sepal.Width$range[1], min(datadf$Sepal.Width))
   expect_equal(hdfSumm$Sepal.Width$range[2], max(datadf$Sepal.Width))
   expect_equal(hdfSumm$Sepal.Width$stats$mean, mean(datadf$Sepal.Width))
   expect_equal(hdfSumm$Sepal.Width$stats$var, var(datadf$Sepal.Width))
   
   # summaries with NA
   expect_equal(hdfSumm$Sepal.Length$range[1], min(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(hdfSumm$Sepal.Length$range[2], max(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(hdfSumm$Sepal.Length$stats$mean, mean(datadf$Sepal.Length, na.rm = TRUE))
   expect_equal(hdfSumm$Sepal.Length$stats$var, var(datadf$Sepal.Length, na.rm = TRUE))
   # expect_equal(hdfSumm$Sepal.Length$stats$skewness, skewness(datadf$Sepal.Length))
   # expect_equal(hdfSumm$Sepal.Length$stats$kurtosis, kurtosis(datadf$Sepal.Length, type = 2))

   hdfSumm
})

############################################################################
############################################################################
context("hdfs divide() checks")

test_that("conditioning division and bsv", {
   path2 <- file.path("/tmp", "hdd_test_div")
   try(rhdel(path2), silent = TRUE)
   
   hdd <- divide(hdf, by = "Species", output = hdfsConn(path2, autoYes = TRUE), update = TRUE,
      bsvFn = function(x)
         list(meanSL = bsv(mean(x$Sepal.Length))))
   
   # do some additional checks here...
   expect_true(hasExtractableKV(hdd))
   
   keys <- sort(unlist(getKeys(hdd)))
   expect_true(keys[1] == "Species=setosa")
   
   # TODO: check print method output more closely
   hdd
})

test_that("random replicate division", {
   path2 <- file.path("/tmp", "hdd_test_rrdiv")
   try(rhdel(path2), silent = TRUE)
   
   hdr <- divide(hdf, by = rrDiv(nrow=200), output = hdfsConn(path2, autoYes = TRUE), postTransFn = function(x) { x$vowel <- as.integer(x$fac %in% c("a", "e", "i", "o", "u")); x })
   hdr
})

############################################################################
############################################################################
context("hdfs recombine() checks")

hdd <- ddf(hdfsConn(file.path("/tmp", "hdd_test_div")))
mpw <- mean(hdd[[1]][[2]]$Petal.Width)

test_that("simple recombination", {
   res <- recombine(hdd, apply = function(v) mean(v$Petal.Width))
   ind <- which(sapply(res, function(x) x[[1]] == "Species=setosa"))
   expect_equal(res[[ind]][[2]], mpw)
})

test_that("recombination with combRbind", {
   res <- recombine(hdd, apply = function(v) mean(v$Petal.Width), comb = combRbind())
   expect_equal(res$val[res$Species=="setosa"], mpw)
})

test_that("recombination with combDdo", {
   meanApply <- function(v) {
      data.frame(mpw=mean(v$Petal.Width), mpl=mean(v$Petal.Length))
   }

   res <- recombine(hdd, apply=meanApply, comb=combDdo())
   
   expect_true(inherits(res, "ddo"))
})

# # TODO: figure out why RHIPE doesn't like this
# test_that("recombination with drGLM", {
#    set.seed(1234)
#    hdr <- divide(hdf, by = rrDiv(nrow=200), output = hdfsConn(path2, autoYes = TRUE), postTransFn = function(x) { x$vowel <- as.integer(x$fac %in% c("a", "e", "i", "o", "u")); x })
#    
#    a <- recombine(hdr, 
#       apply = drGLM(vowel ~ Petal.Length, family = binomial()), 
#       combine = combMeanCoef())
# })

############################################################################
############################################################################
context("hdfs conversion checks")

test_that("to memory", {
   hdfMem <- convert(hdf, NULL)
   expect_true(nrow(hdfMem) == 3750)
})

test_that("to local disk", {
   path <- file.path(tempdir(), "hdd_test_convert")
   hdfDisk <- convert(hdd, localDiskConn(path, autoYes=TRUE))
   expect_true(nrow(hdfDisk) == 3750)
})

}




