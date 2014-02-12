\name{lapply}
\alias{lapply}
\title{Apply a function to all key/value pairs of a ddo/ddf object}
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
a <- divide(iris, by="Species")
b <- lapply(a, function(x) x$Sepal.Width)
}

