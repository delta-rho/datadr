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

tabulateReduce(result, reduce.values, maxUnique = NULL)

calculateMoments(y, order = 1, na.rm = TRUE)

combineMoments(m1, m2)

combineMultipleMoments(...)

moments2statistics(m)
}
\arguments{
  \item{formula}{a formula to be used in
  \code{\link{xtabs}}}

  \item{data}{a subset of a 'ddf' object}

  \item{maxUnique}{the maximum number of unique
  combinations of variables to obtaion tabulations for.
  This is meant to help against cases where a variable in
  the formula has a very large number of levels, to the
  point that it is not meaningful to tabulate and is too
  computationally burdonsome.  If \code{NULL}, it is
  ignored.  If a positive number, only the top and bottom
  \code{maxUnique} tabulations by frequency are kept.}

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
Functions that are used to tabulate categorical variables
and compute moments for numeric variables inside through
the MapReduce framework.  Used in
\code{\link{updateAttributes}}.
}

