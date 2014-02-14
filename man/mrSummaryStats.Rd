\name{tabulateMap}
\alias{calculateMoments}
\alias{combineMoments}
\alias{combineMultipleMoments}
\alias{moments2statistics}
\alias{tabulateMap}
\alias{tabulateReduce}
\title{Functions to Compute Summary Statistics in MapReduce}
\usage{
tabulateMap(formula, data)

tabulateReduce(result, reduce.values)

calculateMoments(y, order = 1, na.rm = TRUE)

combineMoments(m1, m2)

combineMultipleMoments(...)

moments2statistics(m)
}
\arguments{
  \item{formula}{a formula to be used in
  \code{\link{xtabs}}}

  \item{data}{a subset of a 'ddf' object Functions that are
  used to tabulate categorical variables and compute
  moments for numeric variables inside through the
  MapReduce framework.  Used in
  \code{\link{updateAttributes}}.}

  \item{result,reduce.values}{inconsequential
  \code{tabulateReduce} parameters}

  \item{y,order,na.rm}{inconsequential
  \code{calculateMoments} parameters}

  \item{m1,m2}{inconsequential \code{combineMoments}
  parameters}

  \item{\ldots}{inconsequential parameters}

  \item{m}{inconsequential \code{moments2statistics}
  parameters}
}
\description{
Functions to Compute Summary Statistics in MapReduce
}

