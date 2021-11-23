# hs12-historic-series

The goal of hs92-historic-series is to create curated international trade data since 2002. This uses the HS12 (aka H4 in UN COMTRADE) trade classification with 6 digits depth level.

Before 2012, this data uses HS02 (aka H2) data. The source for this data is UN COMTRADE.

Instead of using CSV or RDS, the chosen format here is Apache Arrow, to force cross language and systems compatibility.
