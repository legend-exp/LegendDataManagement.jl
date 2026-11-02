# Configuration and LegendData

[`LegendData`](@ref) is the entry point to the data and metadata of one experimental setup. It is built from a data configuration that maps path prefixes (`tier`, `metadata`, `par`, ...) to directories. This page describes the configuration file, the environment variable that selects it, and the properties of `LegendData`.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## Configuration Files

A configuration file is a YAML or JSON file, chosen by the file extension. It has the top-level entry `setups`, which holds one entry per setup. Each setup has a `paths` table that maps path prefixes to directories:

```yaml
setups:
  l200:
    paths:
      metadata: $_/legend-metadata/
      tier: $_/generated/tier/
      tier/jlrep: $_/generated/jlreport/
      tier/jlplt: $_/generated/jlplt/
      par: $_/generated/jlpar/
```

Inside string values, `$_` stands for the directory of the configuration file and `$NAME` for the environment variable `NAME`. An undefined variable is an error. Keys are not substituted.

The prefixes that LDM uses are

* `metadata`: the metadata database, see [Metadata and Validity](@ref),
* `tier`: the tier files, see [Data Tiers and Files](@ref),
* `par` and `jlpar`: the parameter databases, see [Parameters and Property Files](@ref).

The data processing in Juleana.jl additionally uses the prefixes `tier/jllog`, `tier/jlrep` and `tier/jlplt` for logs, reports and plots. LDM does not restrict the prefixes: any path below a configured prefix can be resolved.

[`LegendDataConfig`](@ref) reads a file and holds one [`SetupConfig`](@ref) per setup in its field `setups`:

```@example ldm
dir = mktempdir()
write(joinpath(dir, "config.yaml"), raw"""
setups:
  l200:
    paths:
      metadata: $_/legend-metadata/
      tier: $_/generated/tier/
      tier/jlrep: $_/generated/jlreport/
      par: $_/generated/jlpar/
""");

config = LegendDataConfig(joinpath(dir, "config.yaml"))
setup = config.setups.l200;
[join(prefix, "/") => relpath(path, dir) for (prefix, path) in setup.paths]
```

### Resolving Paths

[`data_path`](@ref) resolves path components against the prefixes of a setup. The components are split at `/`. The most specific configured prefix that matches the start of the path is used; the remaining components are appended to its directory. A path without a matching prefix is an `ArgumentError`.

```@example ldm
relpath(data_path(setup, "tier", "raw", "cal"), dir)
```

The prefix `tier/jlrep` takes precedence over `tier`:

```@example ldm
relpath(data_path(setup, "tier", "jlrep", "cal"), dir)
```

```@example ldm
relpath(data_path(setup, "tier/jlrep/cal"), dir)
```

```@example ldm
try data_path(setup, "log") catch err sprint(showerror, err) end
```

`data_path` also accepts a [`LegendData`](@ref) in place of the setup. The result is the same as for its configuration. `scratch_dir` is a temporary directory for the files that the examples write.

```@example ldm
relpath(data_path(l200, "par", "rpars"), scratch_dir)
```

## The Environment Variable `LEGEND_DATA_CONFIG`

`LegendDataConfig()` and `LegendData(:l200)` read the configuration files that are named by the environment variable `LEGEND_DATA_CONFIG`. It is set in the shell startup file or in the Julia session before the `LegendData` object is built. If it is not set, an `ErrorException` is thrown.

```bash
export LEGEND_DATA_CONFIG=/path/to/production/config.yaml
```

The variable may hold several file names separated by colons. The files are merged: for a prefix that occurs in several files, the first file in the list wins (as for `PATH`). This allows to override single directories of a production, for example to read the raw tier from another disk:

```@example ldm
write(joinpath(dir, "overwrite.yaml"), raw"""
setups:
  l200:
    paths:
      tier/raw: /mnt/fast/raw
""");

files = join([joinpath(dir, "overwrite.yaml"), joinpath(dir, "config.yaml")], ":")
withenv("LEGEND_DATA_CONFIG" => files) do
    data = LegendData(:l200)
    (raw = data.tier[:raw, :cal, :p02], jldsp = relpath(data.tier[:jldsp], dir))
end
```

## LegendData

`LegendData(setup::Symbol; dataset::Symbol = :default)` builds the object for the setup of the given name from `LEGEND_DATA_CONFIG`. A setup name that the configuration does not contain throws a `KeyError`.

```julia
using LegendDataManagement

l200 = LegendData(:l200)
```

A `LegendData` can also be built from a `SetupConfig` that was read by hand. The setup name and the dataset are then given explicitly:

```@example ldm
data = LegendData(setup, :l200, :default);
data.name
```

### Properties

`LegendData` has no accessible fields. Its data is reached through the properties

| Property | Content |
|:---------|:--------|
| `l200.metadata` | the metadata database, a [`LegendDataManagement.PropsDB`](@ref), see [Metadata and Validity](@ref) |
| `l200.par` | the parameter database `par`, a `PropsDB`, see [Parameters and Property Files](@ref) |
| `l200.jlpar` | the parameter database `jlpar`, a `PropsDB` |
| `l200.tier` | the paths of the tier directories and files, a [`LegendDataManagement.LegendTierData`](@ref), see [Data Tiers and Files](@ref) |
| `l200.name` | the name of the setup, a `Symbol` |
| `l200.dataset` | the name of the dataset, a `Symbol` |

```@example ldm
(l200.name, l200.dataset)
```

```@example ldm
keys(l200.metadata)
```

`metadata`, `par` and `jlpar` read the directory configured for the prefix of the same name when they are accessed. A prefix that is not configured throws an `ArgumentError`, and so does a path that is not a directory. Each access builds a new `PropsDB`. The directory `jldataprod/overrides/<name>` of the metadata is the override directory of `par` and `jlpar` (and of `metadata`), see [Override Directories](@ref).

```@example ldm
try l200.jlpar catch err sprint(showerror, err) end
```

The fields of the `LegendData` structure (`_config`, `_name`, `_dataset`) and the function `LegendDataManagement.get_setup_config` are internal. The configuration of a `LegendData` is reached through [`data_path`](@ref), not through the fields.

### Datasets

The keyword `dataset` names a run list in the metadata `datasets/runlists`. [`runinfo`](@ref) and [`analysis_runs`](@ref) read it and restrict the runs to those of the list. With the default `:default`, no run list is used and `runinfo` holds every run that the metadata describes.

```@example ldm
keys(l200.metadata.datasets.runlists)
```

```@example ldm
valid = LegendData(:l200; dataset = :valid)
(default = length(runinfo(l200)), valid = length(runinfo(valid)))
```

A dataset that is not in the run lists is detected when the run list is first read:

```@example ldm
try runinfo(LegendData(:l200; dataset = :unknown)) catch err sprint(showerror, err) end
```

A `dataset` entry in a setup of the configuration file is stored in the field `dataset` of the [`SetupConfig`](@ref), but `LegendData` does not use it; the dataset is selected with the keyword only.
