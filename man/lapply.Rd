\docType{methods}
\name{lapply,ddf,function-method}
\alias{lapply,ddf,function-method}
\alias{lapply,ddo,function-method}
\title{Apply a function to all key/value pairs of a ddo/ddf object}
\usage{
\S4method{lapply}{ddf,function}(X, FUN)

\S4method{lapply}{ddo,function}(X, FUN)
}
\arguments{
  \item{X}{the ddo/ddf object to apply the function to}

  \item{FUN}{the function to be applied to the value of
  each key/value pair}
}
\value{
a new ddo object
}
\description{
This function creates a new ddo object by applying a
function to all key/value pairs of a ddo/ddf object.
}
\examples{
bySpecies <- divide(iris, by="Species")
lapply(bySpecies, function(x) x$Sepal.Width)
}

