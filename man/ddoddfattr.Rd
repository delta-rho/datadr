\name{setAttributes}
\alias{getAttribute}
\alias{getAttributes}
\alias{getAttributes.ddf}
\alias{getAttributes.ddo}
\alias{hasAttributes}
\alias{hasAttributes.ddf}
\alias{setAttributes}
\alias{setAttributes.ddf}
\alias{setAttributes.ddo}
\title{Managing attributes of 'ddo' or 'ddf' objects}
\usage{
setAttributes(obj, ...)

\method{setAttributes}{ddf}(obj, attrs)

\method{setAttributes}{ddo}(obj, attrs)

getAttribute(obj, attrName)

getAttributes(obj, ...)

\method{getAttributes}{ddf}(obj, attrNames)

\method{getAttributes}{ddo}(obj, attrNames)

hasAttributes(obj, ...)

\method{hasAttributes}{ddf}(obj, attrNames)
}
\arguments{
  \item{attrs}{a named list of attributes to set}

  \item{obj}{'ddo' or 'ddf' object}

  \item{attrName}{name of the attribute to get}

  \item{\ldots}{additional arguments}

  \item{attrNames}{vector of names of the attributes to
  get}
}
\description{
Managing attributes of 'ddo' or 'ddf' objects
}

