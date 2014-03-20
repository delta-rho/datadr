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
   
   expect_equivalent(head(a[[7]][[2]][,1:4]), head(iris)[,1:4])
   expect_equivalent(nrow(a), nrow(iris))
})

unlink(csvFile)
unlink(file.path(tempdir(), "irisText"), recursive = TRUE)
unlink(file.path(tempdir(), "irisText2"), recursive = TRUE)



