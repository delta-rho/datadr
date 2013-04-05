\name{makeKeys}
\alias{makeKeys}
\title{Construct Split Keys}
\usage{
  makeKeys(obj, vals)
}
\arguments{
  \item{obj}{an object of class 'rhDiv'}

  \item{vals}{a data.frame of values to use to construct
  keys}
}
\description{
  Construct split keys for divided data
}
\details{
  A split key for divided data is a string that identifies
  the split.  For example, if a data set was conditioned on
  "var1" and "var2", then a split corresponding to var1="a"
  and var2="b" would have the key "var1=a|var2=b".  This
  function constructs a vector of keys based on an input
  data.frame specifying the key levels you want to extract
  data for.
}
\author{
  Ryan Hafen
}

