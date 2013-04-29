\name{drGLM}
\alias{drGLM}
\title{GLM Recombination 'apply' Method}
\usage{
  drGLM(...)
}
\arguments{
  \item{ldots}{arguments you would pass to the
  \code{\link{glm}} function}
}
\description{
  GLM recombination method
}
\details{
  This is an experimental prototype.  It provides an
  function to be called for each subset in a recombination
  MapReduce job that applies R's glm method and outputs the
  coefficients.  It is to be passed as the argument
  \code{method} to \code{\link{recombine}}.
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{divide}}, \code{\link{recombine}},
  \code{\link{rrDiv}}
}

