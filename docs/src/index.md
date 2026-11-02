# LegendDataManagement.jl

LegendDataManagement.jl (LDM) provides access to the data and metadata of the LEGEND experiment. It locates data files by setup, tier, period, run and detector, reads the metadata and parameter databases with their validity rules, and defines the identifiers (periods, runs, file keys, detector IDs) that the Julia packages of the LEGEND software stack share. The Juleana.jl data processing builds on it.

```@setup ldm
include(joinpath(@__DIR__, "testdata_setup.jl"))
```

## Installation

LDM is registered in the LEGEND Julia registry. The registry is added once to the Julia installation, together with the General registry:

```julia
using Pkg
pkg"registry add General https://github.com/legend-exp/LegendJuliaRegistry.git"
pkg"add LegendDataManagement"
```

The command is also stored in the script `add_registries.jl` in the root of the repository. The packages `LegendHDF5IO`, `Plots` and `SolidStateDetectors` activate package extensions when they are loaded, see [Extensions](@ref). Helpers of the data processing are in the submodule `LegendDataManagement.LDMUtils`, see [Reports and Utilities](@ref).

## Getting Started

All data access starts from a [`LegendData`](@ref) object. It is built from a configuration file that maps the directories of a data production (tier files, metadata, parameters, logs, plots) to path prefixes. The environment variable `LEGEND_DATA_CONFIG` names this file, see [Configuration and LegendData](@ref).

```julia
using LegendDataManagement

l200 = LegendData(:l200)
```

The examples in this documentation use an `l200` object that is built on the LEGEND test data. The metadata of the test data describes the run `p02-r006`:

```@example ldm
filekey = start_filekey(l200, (:p02, :r006, :cal))
```

```@example ldm
channelinfo(l200, filekey).detector
```

The tier files of the test data belong to other runs; `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files. The path of a tier file follows from the file key, here relative to the tier directory:

```@example ldm
relpath(l200.tier[:jldsp, fk], l200.tier[])
```

## Manual

```@contents
Pages = [
    "man/legenddata.md",
    "man/identifiers.md",
    "man/tiers_files.md",
    "man/metadata_validity.md",
    "man/channel_run_info.md",
    "man/exposure.md",
    "man/parameters.md",
    "man/read_data.md",
    "man/propfuncs.md",
    "man/reports_utils.md",
    "man/extensions.md",
    "man/juleana.md",
]
Depth = 2
```

## Tutorials

```@contents
Pages = [
    "tutorials/energy.md",
    "tutorials/aoe.md",
]
Depth = 1
```
