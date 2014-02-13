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
   expect_error(quantile(ldd, var = "Sepal.Length", tails = 0))
})

ldd <- ddf(conn, update = TRUE)

test_that("result matches quantile()", {
   ### compare quantile.ddf() output to default quantile() method
   sq <- quantile(ldd, var = "Sepal.Length", tails = 0)
   qq <- quantile(tmp$Sepal.Length, probs = seq(0, 1, by = 0.005), type=1)
   
   # plot(sq$fval, sq$q)
   # plot(seq(0, 1, by=0.005), qq)

   expect_true(mean(abs(qq - sq$q)) < 0.0001)
})

test_that("tails check", {
   sls <- sort(tmp$Sepal.Length)
   sq <- quantile(ldd, var="Sepal.Length", tails = 200)
   expect_true(all(head(sls, 200) == head(sq$q, 200)))
})

test_that("by=TRUE", {
   sq2 <- quantile(ldd, var = "Sepal.Length", by = "Species", tails = 0)
   # true quantiles
   tmpd <- divide(tmp, by="Species")
   tmp2 <- recombine(tmpd, 
      apply = function(x) 
         data.frame(fval = seq(0, 1, by = 0.005),
            q = quantile(x$Sepal.Length, 
               probs = seq(0, 1, by = 0.005), type = 3)),
      combine = combRbind())

   tmp2 <- ddply(tmp, .(Species), function(x) 
      data.frame(fval = seq(0, 1, by = 0.005), 
         q = quantile(x$Sepal.Length, 
         probs = seq(0, 1, by = 0.005), type = 3)))
   
   # library(lattice)
   # xyplot(q ~ fval, groups = group, data = sq2, auto.key = TRUE, type = c("p", "g"))
   # xyplot(q ~ fval, groups = Species, data = tmp2, auto.key = TRUE, type = c("p", "g"))
   # plot(sq2$q - tmp2$q)
   
   expect_true(mean(abs(sq2$q - tmp2$q)) < 0.001)
})


