# Extensions

LegendDataManagement has three package extensions that are loaded automatically when the corresponding packages are loaded in the same session: `LegendHDF5IO.jl` (with `LegendDataTypes.jl`) for reading `lh5` files, `SolidStateDetectors.jl` for detector simulations from LEGEND metadata, and `Plots.jl` for plot recipes. Without these packages, the functions that an extension provides have no methods.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

## LegendHDF5IO Extension

With [LegendHDF5IO.jl](https://github.com/legend-exp/LegendHDF5IO.jl) loaded, [`read_ldata`](@ref) reads the data of a tier from `lh5` files. The data is selected by tier, file key, run, period, partition or detector, and can be restricted to columns or filtered with a property function. The selector forms, keywords and examples are described in [Reading Data](@ref).

```julia
using LegendDataManagement, LegendHDF5IO
```

In the examples, `fk` is a file key of the test data with tier files on disk (period 18, run 0, category `cal`) and `det` a detector ID with data in these files.

```@example ldm
tbl = read_ldata((:e_cusp, :timestamp), l200, :jldsp, fk, det)
first(tbl, 3)
```

The extension also lets LegendHDF5IO store the data selectors [`ExpSetup`](@ref), [`DataTier`](@ref), [`DataPeriod`](@ref), [`DataRun`](@ref), [`DataCategory`](@ref), [`DataPartition`](@ref), [`Timestamp`](@ref), [`FileKey`](@ref), [`ChannelId`](@ref) and [`DetectorId`](@ref), and arrays of them, in `lh5` files. They are saved as strings (a [`DetectorId`](@ref) as its integer encoding) with an attribute that names the type, and are read back as the original type.

In this example, `scratch_dir` is a temporary directory for the files that the examples write.

```@example ldm
file = joinpath(scratch_dir, "selectors.lh5")

lh5open(file, "w") do f
    f["filekey"] = fk
    f["detectors"] = [DetectorId(:V97061A), DetectorId(:B93099C)]
end

lh5open(file) do f
    f["filekey"], f["detectors"][:]
end
```

## SolidStateDetectors Extension

With [SolidStateDetectors.jl](https://github.com/JuliaPhysics/SolidStateDetectors.jl) loaded, the types `SolidStateDetector` and `Simulation` can be constructed from the diode and crystal metadata of a [`LegendData`](@ref) object. The detector is configured with the LEGEND defaults, which include the `ADLChargeDriftModel2016`. The floating-point type is `Float32` unless it is given as a type parameter, as in `SolidStateDetector{Float64}(...)`.

```julia
using LegendDataManagement, SolidStateDetectors
```

```@example ldm
using SolidStateDetectors, Plots

sd = SolidStateDetector(l200, :V99000A; verbose = false)
```

The constructor reads the diode metadata of the detector and the metadata of the crystal that is named like the detector without its last character (`V99000` for `V99000A`) from `l200.metadata.hardware.detectors.germanium`. With `verbose = true` (the default) it prints the values that it uses and whether they are defaults. The detector can be plotted with the plot recipes of SolidStateDetectors.jl; `st = :slice` draws a 2D slice.

```@example ldm
plot(sd; st = :slice, aspect_ratio = 1)
```

`Simulation` takes the same arguments and returns a `Simulation` with the electric potential, electric field and weighting potentials not yet calculated. They are calculated with the functions of SolidStateDetectors.jl.

```@example ldm
sim = Simulation(l200, :V99000A; verbose = false)
```

### Environment and Keywords

The optional [`HPGeEnvironment`](@ref) argument sets the medium and the temperature; the default is a vacuum cryostat at 78 K.

```@example ldm
using Unitful

sim = Simulation(l200, :V99000A, HPGeEnvironment("LAr", 87u"K");
    n_thickness = 0.7u"mm", operational_voltage = 4u"kV", verbose = false)
```

If the metadata offers several values or none, the detector is configured with the following priority:

* Thickness of the n⁺ contact: the keyword `n_thickness` (in `mm` unless given with units), then the value of the 0νββ analysis, then the value of the manufacturer, then 1 mm.
* Operational voltage: the keyword `operational_voltage` (in `V` unless given with units), then the recommended voltage of the L200 characterization, then the value of the manufacturer, then 5000 V.
* Impurity density: the impurity curve of the crystal metadata, with the corrections `scale` and `offset` unless `use_impurity_corrections = false`; a constant density of zero if the crystal metadata has no curve for the slice of the detector.

Further keywords are `allow_cylindrical_asymmetry` (default `true`), which cuts features such as a crack that break the azimuthal symmetry out of the detector and switches the grid to a full 3D azimuthal range, `verbose`, and `dicttype` for the type of the configuration dictionary. The full list is in the docstrings of the constructors, which are listed on the API page under the extensions.

### Metadata Files and Configuration Files

A detector can also be created from metadata files, without a [`LegendData`](@ref) object and without the `LEGEND_DATA_CONFIG` environment variable. The type `LegendData` is passed as first argument, followed by the path of the diode file and optionally the path of the crystal file. The floating-point type has to be given as type parameter for this form. Without a crystal file a warning is logged and the impurity density is a constant zero.

```@example ldm
meta_dir = joinpath(testdata_dir, "data", "legend", "metadata", "hardware", "detectors", "germanium")
sd = SolidStateDetector{Float32}(LegendData,
    joinpath(meta_dir, "diodes", "V99000A.yaml"), joinpath(meta_dir, "crystals", "V99000.yaml");
    verbose = false)
sd.name
```

The configuration that the extension builds for SolidStateDetectors.jl is created in memory. With the keyword `ssd_config_filename` it is also written to a YAML file, which can be edited and used independently of LegendDataManagement.

```@example ldm
config_file = joinpath(scratch_dir, "V99000A_ssd.yaml")
SolidStateDetector(l200, :V99000A; ssd_config_filename = config_file, verbose = false)
first(readlines(config_file), 6)
```

```julia
sim = Simulation{Float32}(config_file)
```

!!! warning
    A simulation from a modified configuration file can behave differently from the LEGEND defaults. The format of the file is described in the [SolidStateDetectors.jl documentation](https://juliaphysics.github.io/SolidStateDetectors.jl/stable/man/config_files/).

### Running a Simulation

The fields of a `Simulation` are calculated and stored with the functions of SolidStateDetectors.jl, and with LegendHDF5IO loaded they can be written to and read from `lh5` files. The calculation takes minutes to hours, so this example is not run here.

```julia
using LegendDataManagement, SolidStateDetectors, LegendHDF5IO

l200 = LegendData(:l200)
sim = Simulation(l200, :V99000A)
simulate!(sim)
ssd_write("V99000A.lh5", sim)
sim_in = ssd_read("V99000A.lh5", Simulation)
```

## Plots Extension

With [Plots.jl](https://github.com/JuliaPlots/Plots.jl) loaded, `LegendDataManagement` defines three plot recipes, which are used through `plot`. This extension is independent of [LegendMakie.jl](https://github.com/legend-exp/LegendMakie.jl), which provides its own Makie-based plots for LEGEND data.

### Parameter Overview

`plot(chinfo, pars, properties)` plots one parameter for every detector of a channel table `chinfo` (see [Channel, Run and Partition Information](@ref)). The detectors are grouped by string (`detstring`), ordered by `position`, and labeled on the x axis. The value is `pars[detector]` followed by the property path `properties`, e.g. `[:fwhm]`, with its uncertainty drawn as error bars. Detectors without a non-zero value are drawn as `NaN` and labeled in red. If `properties` is empty, `pars[detector]` has to be the value itself. The keyword `calculate_mean` (default `true`) adds the (weighted) mean to the legend, and `verbose` (default `false`) logs a warning for each detector without a value.

```@example ldm
using PropDicts, Measurements

chinfo = channelinfo(l200, mfk; system = :geds)
pars = PropDict(Dict(:V99000A => Dict(:fwhm => measurement(2.6, 0.1) * u"keV")))

plot(chinfo, pars, [:fwhm]; size = (600, 400), thickness_scaling = 1)
```

### Event Display

The waveforms of a single event are plotted with the timestamp of the event, given as a `Unitful.Time` (seconds since the Unix epoch) or as a `Dates.DateTime`. The data is read with [`read_ldata`](@ref) from the tier `plot_tier` (default `DataTier(:raw)`), so the tier files have to be available.

```julia
using LegendDataManagement, LegendHDF5IO, Plots, Unitful

l200 = LegendData(:l200)
ts = 1.6785791257987175e9u"s"
ch = ChannelId(1104000)

plot(l200, ts, ch)                 # waveforms of one channel
plot(l200, ts)                     # event display; the channels are found from the data
plot(l200, filekey, ts, ch)        # with a given file key `filekey`
```

* For a channel, the waveform properties `plot_waveform` (default `[:waveform_presummed]`) are drawn. If the file key is not given, it is found with [`find_filekey`](@ref). The keyword `show_unixtime = true` shows the Unix time in the title.
* Without a channel, the recipe looks the event up in the data. For a `cal` event, the waveform of the HPGe channel that triggered is drawn, and an error is thrown if that channel is not an HPGe channel, or not processable with `only_processable = true` (default). For a `phy` event, the waveforms of all channels of each system in `system` are drawn, one subplot per system. `system` maps system names to the waveform properties; the default is `Dict(:geds => [:waveform_presummed], :spms => [:waveform_bit_drop])`.

## Makie Plots with LegendMakie

[LegendMakie.jl](https://github.com/legend-exp/LegendMakie.jl) has its own extension for LegendDataManagement, which is loaded together with LegendMakie and a Makie backend. It provides the same two kinds of plots through `lplot`. These methods are defined and documented in LegendMakie.jl.

```julia
using LegendDataManagement, LegendHDF5IO
using LegendMakie, CairoMakie

l200 = LegendData(:l200)
filekey = FileKey("l200-p03-r000-cal-20230311T235840Z")

pars = l200.par.ppars.ecal(filekey)
chinfo = channelinfo(l200, filekey; system = :geds, only_processable = true)

fig = lplot(chinfo, pars, [:e_cusp_ctc, :fwhm])
```

`lplot(chinfo, pars, properties)` takes the same arguments as the parameter overview recipe above: the channel table `chinfo` with the detectors on the x axis, a `PropDict` `pars` with the detector IDs as keys, and the property path `properties` (empty if `pars[detector]` is the value itself).

```julia
using Unitful

ts = 1.6785791257987175e9u"s"

fig = lplot(l200, ts)                       # event display; the channels are found from the data
fig = lplot(l200, ts, DetectorId(:V99000A)) # waveforms of one detector
```

* With a detector (a [`DetectorId`](@ref) or a detector name), the waveform properties `plot_waveform` (default `[:waveform_presummed]`) of the tier `plot_tier` (default `DataTier(:raw)`) are drawn.
* Without a detector, the event is looked up in the data as for the Plots recipe. For a `cal` event, the triggered HPGe detector is drawn (it has to be processable with `only_processable = true`, the default); for a `phy` event, all channels of the systems in `system` (default `[:geds, :spms]`) are drawn.
