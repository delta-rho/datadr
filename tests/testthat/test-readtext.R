context("read from text file checks")

csvFile <- file.path(tempdir(), "iris.csv")
write.csv(iris, file = csvFile, row.names = FALSE, quote = FALSE)

test_that("read local text", {
  irisTextConn <- localDiskConn(file.path(tempdir(), "irisText"), autoYes = TRUE)

  a <- readTextFileByChunk(input = csvFile,
    output = irisTextConn, linesPerBlock = 10,
    fn = function(x, header) {
      colNames <- strsplit(header, ",")[[1]]
      read.csv(textConnection(paste(x, collapse = "\n")),
        col.names = colNames, header = FALSE)
    })
  expect_equivalent(head(a[[7]][[2]]), head(iris))
})

test_that("read local text with drRead.csv", {
  irisTextConn <- localDiskConn(file.path(tempdir(), "irisText2"), autoYes = TRUE)

  a <- drRead.csv(csvFile, output = irisTextConn, rowsPerBlock = 10)
  a <- updateAttributes(a)

  tmp <- do.call(rbind, lapply(a[1:15], "[[", 2))
  tmp <- tmp[order(tmp$Species, tmp$Sepal.Length, tmp$Sepal.Width, tmp$Petal.Length, tmp$Petal.Width),]
  iris2 <- iris[order(iris$Species, iris$Sepal.Length, iris$Sepal.Width, iris$Petal.Length, iris$Petal.Width),]
  iris2$Species <- as.character(iris2$Species)
  expect_equivalent(iris2, tmp)
})

unlink(csvFile)
unlink(file.path(tempdir(), "irisText"), recursive = TRUE)
unlink(file.path(tempdir(), "irisText2"), recursive = TRUE)



