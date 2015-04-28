
tmpDat <- data.frame(
  xx = rnorm(1000),
  yy = rnorm(1000),
  by = sample(letters, 1000, replace = TRUE))

data <- divide(tmpDat, by = "by", update = TRUE)

test_that("memory join", {
  res <- drHexbin(data, xVar = "xx", yVar = "yy")
  compare <- hexbin(tmpDat$xx, tmpDat$yy)

  expect_true(all(res@cell == compare@cell))
  expect_true(all(res@count == compare@count))
  expect_true(sqrt(mean((res@xcm - compare@xcm)^2)) < 1e-15)
  expect_true(sqrt(mean((res@ycm - compare@ycm)^2)) < 1e-15)

  # plot(res)
  # plot(compare)
})

