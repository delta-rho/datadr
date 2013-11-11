\name{kvApply}
\alias{kvApply}
\title{Apply Function to Key-Value Pair}
\usage{
  kvApply(fn, kvPair, returnKV = FALSE)
}
\arguments{
  \item{fn}{a function}

  \item{kvPair}{a key-value pair (a list with 2 elements)}

  \item{returnKV}{should the key be added to the result?}
}
\description{
  Apply a function to a key-value pair
}
\details{
  Determines how a function should be applied to a
  key-value pair and then applies it: if the function has 2
  formals, it applies the function giving it the key and
  the value as the arguments; if the function has 1 formal,
  it applies the function giving it just the value.  This
  provides flexibility and simplicity for when a function
  is only meant to be applied to the value, but still
  allows keys to be used if desired.
}

