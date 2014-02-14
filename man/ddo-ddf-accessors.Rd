\name{kvExample}
\alias{bsvInfo}
\alias{counters}
\alias{getKeys}
\alias{kvExample}
\alias{length.ddo}
\alias{names.ddf}
\alias{splitRowDistn}
\alias{splitSizeDistn}
\alias{summary.ddf}
\title{Accessor Functions}
\usage{
kvExample(x, transform = FALSE)

bsvInfo(x)

counters(x)

splitSizeDistn(x)

splitRowDistn(x)

getKeys(x)

\method{summary}{ddf}(object, ...)

\method{names}{ddf}(x)

\method{length}{ddo}(x)
}
\arguments{
  \item{x}{a 'ddf'/'ddo' object}

  \item{transform}{if the 'ddf' object has a
  \code{transFn}, should it be applied prior to returning?}

  \item{object}{a 'ddf'/'ddo' object}

  \item{\ldots}{additional arguments}
}
\description{
Accessor functions for attributes of ddo/ddf objects.
Methods also include \code{nrow} and \code{ncol} for ddf
objects.

Accessor methods for 'ddo' and 'ddf' objects
}

