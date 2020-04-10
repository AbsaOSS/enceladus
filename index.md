---
layout: default
title: Enceladus
---

Enceladus is an open source project under the [ABSA Group Limited][absa] designed for transforming
any data format into a standardized and strongly typed `parquet` format and applying a number of
conformance rules on top of said data.

Enceladus laverages [Apache Spark][spark] to read and transform the data. On top of that it also
uses [Cobrix][gh-cobrix] to be able to read COBOL data, [Spline][gh-spline] to keep track of the
data lineage, and many other open sourrce tools.

Enceladus is shipped as a UI for managing data sets and 2 spark jobs for transforming said data sets.
More about this in the [components][components-tab] tab.

To see all projects made by the AbsaOSS initiative please visit our main [GH Pages][absaoss]

The whole project is under the MIT license.

[absa]: https://www.absa.africa/absaafrica/
[spark]: https://spark.apache.org/
[gh-cobrix]: https://github.com/AbsaOSS/cobrix
[gh-spline]: https://github.com/AbsaOSS/spline
[absaoss]: https://absaoss.github.io/
[components-tab]: {{ site.baseurl }}/docs/components
