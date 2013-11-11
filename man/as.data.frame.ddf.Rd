\name{as.data.frame.ddf}
\alias{as.data.frame.ddf}
\title{Turn "ddf" Object into Data Frame}
\usage{
  as.data.frame.ddf(x, keys = TRUE, splitVars = TRUE,
    bsvs = FALSE, ...)
}
\arguments{
  \item{should}{the key be added as a variable in the
  resulting data frame? (if key is not a character, it will
  be replaced with a md5 hash)}

  \item{splitVars}{should the values of the splitVars be
  added as variables in the resulting data frame?}

  \item{bsvs}{should the values of bsvs be added as
  variables in the resulting data frame? Rbind all the rows
  of a ddf object into a single data frame}
}
\description{
  Turn "ddf" Object into Data Frame
}

