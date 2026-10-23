# Metadata and Validity

The metadata of a LEGEND data production is a directory tree of YAML and JSON files. [`LegendDataManagement.PropsDB`](@ref) presents such a tree as nested dictionaries, and `validity.yaml` files select which of the files apply at a given time and for a given data category.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## Navigating the Metadata

`l200.metadata` is the [`LegendDataManagement.PropsDB`](@ref) of the metadata directory of the setup (see [`LegendData`](@ref)). Sub-directories are accessed as properties and return another `PropsDB`; a file `<name>.yaml` or `<name>.json` is accessed as the property `<name>` and returns a `PropDict` of [PropDicts.jl](https://github.com/oschulz/PropDicts.jl). The parameter databases `l200.par` and `l200.jlpar` are `PropsDB`s as well (see [Parameters and Property Files](@ref)).

```@example ldm
sort(propertynames(l200.metadata))
```

```@example ldm
sort(propertynames(l200.metadata.hardware))
```

Leaf entries are `PropDict`s with dot access to nested properties:

```@example ldm
diodes = l200.metadata.hardware.detectors.germanium.diodes
sort(collect(keys(diodes)))
```

```@example ldm
diodes.V99000A.type
```

```@example ldm
diodes.V99000A.production.mass_in_g
```

`getproperty` is meant for standardized names. `getindex` accepts any `Symbol`, so names held in a variable can be used. Several indices can be chained, and a vector of symbols returns a vector of entries:

```@example ldm
det_name = :V99000A
diodes[det_name].type
```

```@example ldm
diodes[:V99000A, :type]
```

```@example ldm
[d.type for d in diodes[[:V99000A, :B99000A]]]
```

[`data_path`](@ref) gives the directory a `PropsDB` reads from:

```@example ldm
basename(data_path(l200.metadata.hardware.detectors.germanium))
```

A `PropDict` returns a `PropDicts.MissingProperty` for a property that does not exist; `haskey` and `get` check for existence.

!!! note
    The list of property names of a `PropsDB` is read from disk when the `PropsDB` is created. Names of files added later can still be accessed by property or index, but `keys` does not list them.

## Validity Selections

Directories whose content depends on time and data category contain a `validity.yaml` file. A `PropsDB` for such a directory has no content on its own: `keys` throws an `ArgumentError` until a selection is made, and the files that `validity.yaml` refers to are not listed as properties.

```@example ldm
channelmaps = l200.metadata.hardware.configuration.channelmaps
try
    keys(channelmaps)
catch err
    first(split(sprint(showerror, err), " at "))
end
```

The selection is made by calling the `PropsDB` with a [`FileKey`](@ref), a [`LegendDataManagement.ValiditySelection`](@ref) or a timestamp and a data category. A [`FileKey`](@ref) selects its start time and category. The call returns the merged content of the selected files as a `PropDict`. `mfk` is a file key of the toy production in the test metadata (period 2, run 6, category `cal`).

```@example ldm
sort(collect(keys(channelmaps(mfk))))
```

```@example ldm
sel = ValiditySelection("20221226T200846Z", :cal)
channelmaps(sel).V99000A.system
```

```@example ldm
channelmaps("20221226T200846Z", :cal).V99000A.daq.rawid
```

[`LegendDataManagement.AnyValiditySelection`](@ref) is the union of `ValiditySelection` and `FileKey` and is accepted wherever time and category matter, for example by [`channelinfo`](@ref). A timestamp without a category is not a selection.

The selection can be made at any point of the traversal. A call at a directory without `validity.yaml` is passed on to its sub-directories, so the following expressions are equivalent:

```@example ldm
a = l200.metadata.hardware.configuration.channelmaps(mfk)
b = l200.metadata(mfk).hardware.configuration.channelmaps
a == b
```

A time before the first entry of the validity file is an error:

```@example ldm
try
    channelmaps("19990101T000000Z", :cal)
catch err
    sprint(showerror, err)
end
```

## The `validity.yaml` File

A `validity.yaml` file is a list of entries. Each entry has the keys

* `valid_from`: Time from which the entry applies, in the format `yyyymmddTHHMMSSZ`.
* `category`: Data category or list of data categories the entry is for, e.g. `cal`, `phy` or `[cal, phy]`. Default is `all`.
* `mode`: How `apply` changes the file list of the category. One of `reset`, `append`, `remove` and `replace`. Default is `append`.
* `apply`: File name or list of file names, relative to the directory of `validity.yaml`.

Every category has its own timeline. The file list of a category after an entry is derived from the list after the previous entry of the same category:

* `reset`: The list is `apply`.
* `append`: The files of `apply` are added at the end of the list.
* `remove`: The files of `apply` are removed from the list.
* `replace`: `apply` has exactly two files; all occurrences of the first are removed from the list, and the second is added at the end.

The first entry of a category always starts from an empty list, whatever its `mode`. For `replace` the list of the first entry is therefore both files of `apply`. The entries of a category have to be in ascending order of `valid_from`; this is not checked, and an unsorted file gives wrong or failing selections. `remove` and `replace` do not check that the file to remove is in the list.

The selection `(time, category)` uses the last entry of the category with `valid_from` not after `time`. The category `all` is used only if the validity file has no entry of the selected category at all; `all` entries do not extend the timeline of another category. An error is thrown if the validity file has neither.

The files are read in the order of the list and merged, so properties of later files override those of earlier ones. A name of the list that is neither in the directory nor in the override directory (see below) is an error. Files in the directory that no entry refers to are ordinary properties of the `PropsDB`.

The following example writes three property files into a temporary directory `scratch_dir` and a validity file that uses every mode, and selects from it at different times:

```@example ldm
using PropDicts

toy = joinpath(scratch_dir, "toy")
mkpath(joinpath(toy, "gains"))
writeprops(joinpath(toy, "gains", "a.yaml"), PropDict(:gain => 1.0))
writeprops(joinpath(toy, "gains", "b.yaml"), PropDict(:offset => 2.0))
writeprops(joinpath(toy, "gains", "c.yaml"), PropDict(:gain => 3.0))
write(joinpath(toy, "gains", "validity.yaml"), """
- valid_from: 20230101T000000Z   # category all
  apply: a.yaml
- valid_from: 20230201T000000Z   # first entry of cal: list is [a, b]
  category: cal
  apply: [a.yaml, b.yaml]
- valid_from: 20230301T000000Z   # [b, c]
  category: cal
  mode: replace
  apply: [a.yaml, c.yaml]
- valid_from: 20230401T000000Z   # [c]
  category: cal
  mode: remove
  apply: b.yaml
- valid_from: 20230501T000000Z   # append is the default: [c, a]
  category: cal
  apply: a.yaml
- valid_from: 20230601T000000Z   # [b]
  category: cal
  mode: reset
  apply: b.yaml
""")

gains = LegendDataManagement.AnyProps(toy).gains
nothing # hide
```

The category `phy` has no entries, so `all` applies:

```@example ldm
gains("20230315T000000Z", :phy)
```

The category `cal` has its own timeline, shown here for the middle of February to June:

```@example ldm
[gains("2023$(m)15T000000Z", :cal) for m in ("02", "03", "04", "05", "06")]
```

Before its first entry the category `cal` does not fall back to `all`:

```@example ldm
try
    gains("20230115T000000Z", :cal)
catch err
    sprint(showerror, err)
end
```

### Override Directories

[`LegendDataManagement.AnyProps`](@ref) takes a second directory via `override_base`. A file of the base directory is read together with the file of the same relative path in the override directory, which is read later and so takes precedence. The `validity.yaml` files of both directories are merged per category: entries with the same `valid_from` get the concatenated file lists (base first), and all other entries keep the file list that results from their own file. [`LegendData`](@ref) uses `<metadata>/jldataprod/overrides/<name>` as the override directory of the database `<name>`.

```@example ldm
override = joinpath(scratch_dir, "toy_override")
mkpath(joinpath(override, "gains"))
writeprops(joinpath(override, "gains", "c.yaml"), PropDict(:gain => 30.0))

gains_override = LegendDataManagement.AnyProps(toy; override_base = override).gains
gains_override("20230315T000000Z", :cal)
```

## Missing Entries

A property of a `PropsDB` without a corresponding file or directory is not an error. It returns a [`LegendDataManagement.NoSuchPropsDBEntry`](@ref), and a further property access extends its path.

```@example ldm
db = LegendDataManagement.AnyProps(toy)
entry = db.settings.analysis
(typeof(entry), basename(data_path(entry)))
```

`PropDict(entry)` gives an empty `PropDict`, and `PropDicts.writeprops(entry, props)` creates the missing directories and the file `<last name>.yaml`. It refuses to write to a name that is an existing directory.

```@example ldm
isempty(PropDict(entry))
```

```@example ldm
writeprops(entry, PropDict(:threshold => 5))
readdir(joinpath(toy, "settings"))
```

```@example ldm
db.settings.analysis.threshold
```

A missing property of a `PropsDB` that still needs a validity selection throws an `ArgumentError` instead.

!!! note
    Metadata is read-only for an analysis. Writing is meant for the parameter databases `l200.par` and `l200.jlpar`, see [Parameters and Property Files](@ref).

## Data Production Configuration

[`dataprod_config`](@ref) returns the `PropsDB` of the configuration of the data production, the `config` entry of the metadata `jldataprod` or, if there is none, `dataprod`. [`pydataprod_config`](@ref) does the same for `pydataprod`. [`dataprod_parameters`](@ref) and [`pydataprod_parameters`](@ref) return `l200.par`. The configuration is selected with a [`FileKey`](@ref) or a validity selection like any other `PropsDB`. The configuration of a production is organized in sections such as `dsp`, `qc` or `energy`, each selected by the start file key of a run:

```julia
using LegendDataManagement

l200 = LegendData(:l200)
filekey = start_filekey(l200, (:p03, :r000, :cal))

dsp_config = dataprod_config(l200).dsp(filekey)
qc_config = dataprod_config(l200).qc(filekey)
```

The sections depend on the production; the test metadata used on this page has none, so the example is not executed.
