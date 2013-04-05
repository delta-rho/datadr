\name{divJoin}
\alias{divJoin}
\title{Join New Data to a "localDiv" object}
\usage{
  divJoin(obj, newDat, elemName)
}
\arguments{
  \item{obj}{an object of class "localDiv"}

  \item{newDat}{new data / list / function with which to
  join to \code{obj} (see details)}

  \item{elemName}{the name of the new element created by
  the join for each split}
}
\value{
  the input object "obj" with the joined data
}
\description{
  Join new data to a "localDiv" object
}
\details{
  \code{newDat} must be either a list of lists where the
  first element of each sublist matches those keys of
  localDiv, a data.frame with columns with the same names
  as splitVars, or a function that takes an element of
  localDiv as its argument and what it returns gets
  assigned to \code{elemName} of that split.
}
\examples{
irisSplit <- divide(iris, "Species")

# join a data.frame
newDatDF <- data.frame(
   Species=rep(unique(iris$Species), 3),
   val=1:9
)
irisSplit <- divJoin(irisSplit, newDatDF, "testDF")

# join a list
newDatList <- lapply(paste("Species=", unique(iris$Species), sep=""), function(x) list(x, rnorm(5)))
irisSplit <- divJoin(irisSplit, newDatList, "testList")

# "join" using a function
newDatFun <- function(x) max(x$rows$Sepal.Length)
irisSplit <- divJoin(irisSplit, newDatFun, "testFun")
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{divide}}
}

