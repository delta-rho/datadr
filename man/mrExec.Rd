\name{mrExec}
\alias{mrExec}
\title{Execute a MapReduce Job}
\usage{
mrExec(data, setup = NULL, map = NULL, reduce = NULL, output = NULL,
  control = NULL, params = NULL, verbose = TRUE)
}
\arguments{
  \item{data}{a ddo/ddf object, or list of ddo/ddf objects}

  \item{setup}{an expression of R code (created using the R
  command \code{expression}) to be run before map and
  reduce}

  \item{map}{an R expression that is evaluated during the
  map stage. For each task, this expression is executed
  multiple times (see details).}

  \item{reduce}{a vector of R expressions with names pre,
  reduce, and post that is evaluated during the reduce
  stage. For example \code{reduce = expression(pre={...},
  reduce={...}, post={...})}. reduce is optional, and if
  not specified the map output key-value pairs will be the
  result. If it is not specified, then a default identity
  reduce is performed. Setting it to 0 will skip the reduce
  altogether.}

  \item{output}{a "kvConnection" object indicating where
  the output data should reside (see
  \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If
  \code{NULL} (default), output will be an in-memory "ddo"
  object.}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}}
  and \code{\link{localDiskControl}}}

  \item{params}{a named list of parameters external to the
  input data that are needed in the map or reduce phases}

  \item{verbose}{logical - print messages about what is
  being done}
}
\value{
"ddo" object - to keep it simple.  It is up to the user to
update or cast as "ddf" if that is the desired result.
}
\description{
Execute a MapReduce job
}
\author{
Ryan Hafen
}

