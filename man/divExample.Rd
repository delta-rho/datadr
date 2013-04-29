\name{divExample}
\alias{divExample}
\title{Get An Example Subset of Divided Data
Get a subset of a divided data object to test functions on prior to doing computation across the entire data set}
\usage{
  divExample(data, ...)
}
\arguments{
  \item{divided}{data object (e.g. of class "localDiv",
  "rhDdata", etc.)}

  \item{should}{the transformation function specified when
  the data was created be applied?}
}
\value{
  one subset of the divided data
}
\description{
  Get An Example Subset of Divided Data Get a subset of a
  divided data object to test functions on prior to doing
  computation across the entire data set
}
\note{
  \code{divExample(data)} returns the value from one
  key/value pair.  This is the form of the data when used
  in functions like \code{plotFn} or \code{cogFn} in
  \code{\link{vdbPlot}} or what it will look like when used
  in \code{\link{recombine}}.  If you want some sample
  key/value pairs to test some MapReduce code, simply use
  \code{data$example}.
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{divide}}, \code{\link{rhipeDiv}},
  \code{\link{vdbPlot}}
}

