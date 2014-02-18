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


