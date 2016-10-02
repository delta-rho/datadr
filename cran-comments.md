## Test environments

* local OS X install, R 3.3.1
* ubuntu 12.04 (on travis-ci), R 3.3.1
* win-builder (devel and release)

## R CMD check results

There were no ERRORs or WARNINGs.

There were 2 NOTEs:

    * checking CRAN incoming feasibility ... NOTE
    Maintainer: 'Ryan Hafen <rhafen@gmail.com>'

    License components with restrictions and base license permitting such:
      BSD_3_clause + file LICENSE
    File 'LICENSE':
      YEAR: 2016
      COPYRIGHT HOLDER: Battelle Memorial Institute
      ORGANIZATION: Pacific Northwest National Laboratory, Purdue University

    Possibly mis-spelled words in DESCRIPTION:
      HDFS (12:26)
      Hadoop (12:67)
      MapReduce (11:5)
      RHIPE (13:41)
      analytical (9:63)

    Suggests or Enhances not in mainstream repositories:
      Rhipe
    Availability using Additional_repositories specification:
      Rhipe   yes   http://ml.stat.purdue.edu/packages

    * checking package dependencies ... NOTE
    Package suggested but not available for checking: 'Rhipe'

All words are spelled correctly.

The Rhipe R package is not required for any of the core functionality of datadr.  It is an optional back end for datadr functions that ties R to Hadoop.  Rhipe is a Linux-only R package available at http://ml.stat.purdue.edu/packages but since it has specific system-level dependencies (Hadoop, protocol buffers 2.5, etc.), we do not anticipate it being made easily available on CRAN.  Since Rhipe only enhances functionality of datadr when used against a large Hadoop cluster, while all the same functionality runs fine without Rhipe on a local workstation, we believe it is safe to ignore this note.

## Downstream dependencies

There are currently no downstream dependencies for this package.
