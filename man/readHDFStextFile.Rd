\name{readHDFStextFile}
\alias{readHDFStextFile}
\title{Experimental HDFS text reader helper function}
\usage{
readHDFStextFile(input, output = NULL, fn = NULL, keyFn = NULL,
  linesPerBlock = 10000, control = NULL, update = FALSE)
}
\arguments{
  \item{input}{a RHIPE input text handle created with
  \code{rhfmt}}

  \item{output}{an output connection such as those created
  with \code{\link{localDiskConn}}, and
  \code{\link{hdfsConn}}}

  \item{fn}{function to be applied to each chunk of lines
  (input to function is a vector of strings)}

  \item{keyFn}{optional function to determine the value of
  the key for each block}

  \item{linesPerBlock}{how many lines at a time to read}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}}
  and \code{\link{localDiskControl}}}

  \item{update}{should a MapReduce job be run to obtain
  additional attributes for the result data prior to
  returning?}
}
\description{
Experimental helper function for reading text data on HDFS
into a HDFS connection
}

