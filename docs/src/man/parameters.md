# Parameters and Property Files

Analysis results such as calibration constants are stored as JSON or YAML property files in the parameter directories `par` and `jlpar` of a [`LegendData`](@ref) object. This page describes how these files are read and written ([`readlprops`](@ref), [`writelprops`](@ref)), how units and uncertainties are encoded, how the files are organized in `l200.par`, and how a `validity.yaml` file selects the files that apply at a given time.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## Layout of the Parameter Directory

`l200.par` is a [`LegendDataManagement.PropsDB`](@ref) for the directory configured as `par` (see [Configuration and LegendData](@ref)); `l200.jlpar` does the same for `jlpar`. The Juleana dataflow organizes the directory in two trees, each with one sub-directory per parameter category (`ecal`, `ctc`, `pz`, `aoe`, `psd`, `qc`, `sipmcal`, ...):

* `rpars` (run level): one file per run, `rpars/<category>/<period>/<run>.yaml`, holding the parameters of all detectors of the run.
* `ppars` (partition level): one directory per detector, `ppars/<category>/<detector>/<partition>.yaml`, holding the parameters of one detector for all runs of a partition.

The directories are created when the first file is written. The parameter directory of the documentation test production is empty at the start of this page, so the examples write their own parameters. A path that does not exist yet is a `LegendDataManagement.NoSuchPropsDBEntry`, a placeholder that can be written to; further indexing extends the path.

```@example ldm
using PropDicts, Unitful, Measurements

typeof(l200.par.rpars.ecal)
```

## Reading and Writing Property Files

[`readlprops`](@ref) reads a JSON or YAML file into a `PropDicts.PropDict`, [`writelprops`](@ref) writes one. The file format follows the file name extension. Both functions convert between the stored fields `val`, `err` and `unit` and Julia objects:

* `val` and `unit` become a `Unitful.Quantity`.
* `val` and `err` become a `Measurements.Measurement`.
* `val`, `err` and `unit` become a `Unitful.Quantity` of a `Measurements.Measurement`.
* An entry with only `unit` becomes a `NaN` with that unit, and a `null` value of `err` becomes a `NaN` uncertainty.
* The units `none`, `ADC`, `adc` and `sample` and an empty unit are unitless; `e` is the atomic unit of charge and `%` is `percent`. All other units are parsed by Unitful.jl and UnitfulAtomic.jl; an unknown unit throws an `ArgumentError`.
* Arrays are converted element by element. All other entries are unchanged.

A `PropDict` with quantities and measurements is written as follows.

```@example ldm
props = PropDict(
    :tau => 40.0u"μs",
    :fwhm => 2.5u"keV" ± 0.1u"keV",
    :ratio => 0.5 ± 0.01,
    :name => "example",
)

dir = mktempdir()
file = joinpath(dir, "example.yaml")
writelprops(file, props)
print(read(file, String))
```

Reading the file restores the objects.

```@example ldm
readlprops(file)
```

A file written by hand follows the same conventions. The entry `resolution` below has an undefined uncertainty, and `dead` has a unit but no value.

```@example ldm
write(joinpath(dir, "hand.yaml"), """
resolution:
  val: 2.5
  err: null
  unit: keV
offset:
  val: 1
  unit: ADC
dead:
  unit: ms
""")

readlprops(joinpath(dir, "hand.yaml"))
```

[`readlprops`](@ref) also accepts a vector of file names and merges the files in order, so that later files take precedence. JSON cannot hold `NaN`: writing a `PropDict` with a `NaN` to a `.json` file throws an `ArgumentError`, while YAML files store it as `.NaN`.

!!! warning "Keys named `val`"
    [`writelprops`](@ref) and [`readlprops`](@ref) interpret every entry that has the field `val` together with `unit` or `err` as a value. A configuration file that gives another meaning to such a key is altered by a read and write cycle.

### Values and Uncertainties

[`get_values`](@ref) and [`get_uncertainties`](@ref) split the measurements of a `PropDict` or an array. [`get_values`](@ref) removes the uncertainty of every `Measurement` and keeps the unit; [`get_uncertainties`](@ref) returns the uncertainty with the unit. Entries that are not measurements are returned unchanged by both.

```@example ldm
get_values(readlprops(file))
```

```@example ldm
get_uncertainties(readlprops(file))
```

[`lstring`](@ref) returns the single-line JSON text of a `PropDict` as written by [`writelprops`](@ref).

```@example ldm
lstring(PropDict(:fwhm => 2.5u"keV", :n => 2))
```

## Writing Parameters

`writelprops(db, key, props)` writes the file `<key>.yaml` into the directory of a [`LegendDataManagement.PropsDB`](@ref) or `NoSuchPropsDBEntry` `db` and creates the directory if necessary (see [`data_path`](@ref)). Indexing with a period or a detector selects the sub-directory. A run-level parameter file is written with the period as the directory and the run as the key:

```@example ldm
ecal = PropDict(:V99000A => PropDict(:e_cusp => PropDict(:cal => PropDict(
    :func => "e_cusp * 0.25keV + 1.5keV", :fwhm => 2.5u"keV" ± 0.1u"keV"))))

writelprops(l200.par.rpars.ecal[DataPeriod(2)], DataRun(6), ecal)
readdir(data_path(l200.par.rpars.ecal))
```

Indexing a database with two keys reads the file `<first>/<second>.yaml` without any validity selection and returns a `PropDict`. The keys are symbols or data selectors.

```@example ldm
l200.par.rpars.ecal[DataPeriod(2), DataRun(6)]
```

```@example ldm
l200.par.rpars.ecal[:p02, :r006] == l200.par.rpars.ecal[DataPeriod(2), DataRun(6)]
```

The partition level works in the same way with the detector as the directory and the partition as the key; the Juleana processors write `ppars/<category>/<detector>/<partition>.yaml` with `writelprops(l200.par.ppars.ecal[det], part, props)`.

## Validity

A directory of a [`LegendDataManagement.PropsDB`](@ref) may contain a file `validity.yaml` that states which files apply from which time on, separately for each data category. The parameter databases use the same mechanism as the metadata, described in [Metadata and Validity](@ref). An entry has the fields `valid_from` (a timestamp), `category` (default `all`), `apply` (a file name or a list of file names relative to the directory) and `mode`. The modes are `reset` (the list of files is replaced), `append`, `remove` and `replace` (the two files are the one to remove and the one to add). The first entry of a category is always a `reset`.

[`LegendDataManagement.LDMUtils.writevalidity`](@ref) adds such an entry at the start time of a [`FileKey`](@ref); `mfk` below is a file key of the production in the test metadata (period 2, run 6, category `cal`). The `(period, run)` form applies the file `<period>/<run>.yaml`, a [`DataPartition`](@ref) the file `<partition>.yaml`.

```@example ldm
using LegendDataManagement.LDMUtils

writevalidity(l200.par.rpars.ecal, mfk, (DataPeriod(2), DataRun(6)); category = :cal)
print(read(joinpath(data_path(l200.par.rpars.ecal), "validity.yaml"), String))
```

The database is then called with a [`FileKey`](@ref), a [`LegendDataManagement.ValiditySelection`](@ref) or a timestamp and a category. The files that are valid at that time and for that category are read and merged into one `PropDict`.

```@example ldm
l200.par.rpars.ecal(mfk)
```

```@example ldm
l200.par.rpars.ecal("20230101T000000Z", :cal) == l200.par.rpars.ecal(mfk)
```

The first two indices can also select the tree and the category: `l200.par[:rpars, :ecal](mfk)` is `l200.par.rpars.ecal(mfk)`. Juleana code selects the tree with a variable, for example `l200.par[pars_type, :pz](filekey)` with `pars_type` either `:rpars` or `:ppars`.

A selection before the first entry of the category throws an `ArgumentError`, and a category without an entry (and without an entry for `all`) throws an `ErrorException`.

```@example ldm
try
    l200.par.rpars.ecal(ValiditySelection("20210101T000000Z", :cal))
catch err
    println(sprint(showerror, err))
end
```

```@example ldm
try
    l200.par.rpars.ecal(ValiditySelection("20230101T000000Z", :phy))
catch err
    println(sprint(showerror, err))
end
```

### Partitions

In a partition-level database, the validity file of a detector lists one entry per run of the partition, all applying the partition file. [`LegendDataManagement.LDMUtils.get_partitionvalidity`](@ref) returns the start file key of each run and the validity file name for a detector and a partition:

```@example ldm
part = first(keys(partitioninfo(l200, :V99000A, :cal)))
validity = get_partitionvalidity(l200, :V99000A, part)
first(validity, 2)
```

Writing the parameters and the validity of the partition takes one call of [`writelprops`](@ref) and one call of [`LegendDataManagement.LDMUtils.writevalidity`](@ref) for each run:

```@example ldm
db = l200.par.ppars.ecal[:V99000A]
writelprops(db, part, PropDict(:e_cusp => PropDict(:fwhm => 2.0u"keV")))
for run in validity
    writevalidity(db, run.filekey, part; category = :cal)
end
l200.par.ppars.ecal[:V99000A](mfk)
```

The processors of the dataflow use the wrappers `write_pars` and `write_validity` of Juleana.jl (see [Use in Juleana.jl](@ref)), which call these functions.

### Overrides

If the metadata directory contains `jldataprod/overrides/par` (or `jlpar`), the files in it take precedence over files of the same name in the parameter directory, and its `validity.yaml` entries are merged with those of the parameter directory. `l200.par` reads the override directory but writes only to the directory configured as `par`.
