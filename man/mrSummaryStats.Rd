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
\description{
  Functions that are used to tabulate categorical variables
  and compute moments for numeric variables inside through
  the MapReduce framework.  Used in
  \code{\link{updateAttributes}}.
}

