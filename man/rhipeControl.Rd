\name{rhipeControl}
\alias{rhipeControl}
\title{Specify Control Parameters for RHIPE Job}
\usage{
  rhipeControl(mapred = NULL, setup = NULL,
    combiner = FALSE, cleanup = NULL, orderby = "bytes",
    shared = NULL, jarfiles = NULL, zips = NULL,
    jobname = "")
}
\description{
  Specify control parameters for a RHIPE job.  See
  \code{\link{rhwatch}} for details about each of the
  parameters.
}

