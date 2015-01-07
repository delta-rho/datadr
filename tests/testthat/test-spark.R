# not all test environments have Spark installed
TEST_SPARK <- Sys.getenv("DATADR_TEST_SPARK")
if(TEST_SPARK == "")
   TEST_SPARK <- FALSE

if(TEST_SPARK) {


############################################################################
############################################################################
context("spark tests")

test_that("spark MR job", {
   
   permute <- sample(1:150, 150)
   splits <- split(permute, rep(1:3, 50))
   irisSplit <- lapply(seq_along(splits), function(x) {
      list(x, iris[splits[[x]],])
   })
   
   scn <- sparkDataConn(irisSplit)
      # , init = list(master = "spark://localhost:7077"))
   irisDdo <- ddo(scn)
   
   mapExp <- expression({
      lapply(map.values, function(r) {
         by(r, r$Species, function(x) {
            collect(
               as.character(x$Species[1]),
               range(x$Sepal.Length)
            )
         })
      })
   })
   
   reduceExp <- expression(
      pre = {
         rng <- c(Inf, -Inf)
      }, reduce = {
         rx <- unlist(reduce.values)
         rng <- c(min(rng[1], rx, na.rm = TRUE), max(rng[2], rx, na.rm = TRUE))
      }, post = {
         collect(reduce.key, rng)
   })
   
   res <- mrExec(irisDdo, map = mapExp, reduce = reduceExp, output = sparkDataConn())
   
   getAttribute(res, "conn")$data
   
   irisDdo <- updateAttributes(irisDdo)
   irisDdf <- ddf(scn)
   irisDdf <- updateAttributes(irisDdf)
})

}
