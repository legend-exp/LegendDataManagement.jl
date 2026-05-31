# LegendDataManagement.jl

LegendDataManagement.jl provides a Julia implementation of the LEGEND data and metadata management.

The package provides a structured interface to locate, access, and query LEGEND data across data tiers, periods, runs, and channels, and integrates with the LEGEND metadata.


## Configuration

The package expects a configuration file (see the [example "config.yaml"](https://github.com/legend-exp/legend-testdata/blob/main/data/legend/dataflow-config.yaml) in the LEGEND test data repository) describing dataset names, file roots, and partitions. While the path to this configuration file can be specified explicitly, we recommend setting an environment variable named `$LEGEND_DATA_CONFIG` to the absolute path of your "config.json". This can be done in the bashrc or for VisualStudioCode in `terminal.integrated.env.linux"`.

```bash
export LEGEND_DATA_CONFIG="/path/to/legend_data_config.json"
```

LegendDataManagment provides a [SolidStateDetectors extension](@ref) that makes it possible to create `SolidStateDetector` objects from LEGEND metadata.


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


## Documentation structure

- **Examples** provide step-by-step workflows for common data access and
  metadata-management tasks.
- **API** contains the automatically generated reference documentation for
  exported types and functions.

---

## Get started

- Browse the *Manual* for a explanation of basic functions and practical examples.
- Check the *Tutorials* section for ...
- Check *Extenstions* for an explantion about the Package extensions of `Plots`, `LegendHDF5IO` and `SolidStateDetectors`.
- Consult the *API* section for detailed information on selectors and helper
  functions such as `LegendData`, `DataTier`, `DataPeriod`, `search_disk`, and
  `read_ldata`.
