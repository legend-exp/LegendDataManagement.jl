# Data access and discovery

This tutorial demonstrates common workflows for discovering and reading LEGEND
data using LegendDataManagement.jl. It assumes that a valid configuration file
is available and referenced via the `LEGEND_DATA_CONFIG` environment variable.

---

## Setup

Ensure the configuration file is available to Julia:

```bash
export LEGEND_DATA_CONFIG="/path/to/legend_data_config.json"
```

Load the required packages:

```julia
using LegendDataManagement
using LegendHDF5IO
```

---

## Opening a dataset

A dataset is opened by name as defined in the configuration file.

```julia
l200 = LegendData(:l200)
```

The returned `LegendData` object serves as the main entry point for accessing
data tiers and metadata.

---

## Inspecting available data tiers

with the `search_disk` function you can check what kind of data is available to you in the LegendData object:

You can check for available `DataTier` with
``` julia
search_disk(DataTier, l200.tier[])
```
and available `DataCategory` with
``` julia
search_disk(DataCategory, l200.tier[:raw])
```

---

## Data selectors

Data selection is performed using data selectors rather than hard-coded paths.
Common selectors include:

- `DataCategory(:XXX)`
- `DataPeriod(pXX)`
- `DataRun(rXXX)`
- `DetectorID(XYYYYYZ)`

## Reading data

Data can be loaded using `read_ldata`. First, specify the `LegendData` object and `DataTier`, then provide a set of selectors to restrict the data to the subset you want to read.
```julia
data = read_ldata(l200, :jlhit, DataCategory(:cal), DataPeriod(p03), DataRun(r001), DetectorID(:V08682A))
```

More about the read_ldata function can be found in Manual/read_ldata

