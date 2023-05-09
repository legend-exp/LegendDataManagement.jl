# LegendDataManagement.jl

This package provides a Julia implementation of the LEGEND data and metadata management.

It requires a central configuration file (see the [example "config.json"](https://github.com/legend-exp/legend-testdata/blob/main/data/legend/config.json) in the LEGEND test data reposiroty). While the path to this configuration file can be specified explicity, we recommend to set the environment variable `$LEGEND_DATA_CONFIG` to the absulute path of your "config.json".

Usage examples:

```julia
using LegendDataManagement
using PropertyFunctions

l200 = LegendData(:l200)

filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")

raw_filename = l200.tier[:raw, "l200-p02-r006-cal-20221226T200846Z"]

l200.metadata.hardware.detectors.germanium.diodes

chinfo = channel_info(l200, filekey)
filterby(@pf $processable && $usability)(chinfo)
```
