# Extensions

## `Plots` extension

LegendDataManagment provides an extension for [Plots](https://github.com/JuliaPlots/Plots.jl). This makes it possible to directly plot LEGEND data via the `plot` function. The extension is automatically loaded when both packages are loaded.
You can plot a parameter overview as a 2D plot over a set of detectors (requires a `$LEGEND_DATA_CONFIG` environment variable pointing to a legend data-config file):

```julia
using LegendDataManagement, Plots

l200 = LegendData(:l200)

filekey = FileKey("l200-p03-r000-cal-20230311T235840Z")

pars = l200.par.ppars.ecal(filekey)
properties = [:e_cusp_ctc, :fwhm, :qbb];

chinfo = channelinfo(l200, filekey; system = :geds, only_processable = true)

plot(chinfo, pars, properties, verbose = true, color = 1, markershape = :o, calculate_mean = true)
```

The plot recipe takes three arguments:
- `chinfo`: the channel info with all detectors to be plotted on the x-axis
- `pars`: a `PropDict` that has the detector IDs as keys and parameters as values
- `properties`: an array of `Symbols` to access the data that should be plotted
(if no `properties` are provided, the `PropDict` `pars` is expected to just contain the data to be plotted as values)

There are also keyword arguments:
- `calculate_mean`: If set to `true`, then the mean values are included in the legend labels. For values with uncertainties, the mean values are calculated as weighted means.
- `verbose`: some output when the plot is generated, e.g. if values for (some) detectors are missing

A 3D plot is WIP.

In addition, you can plot an event display of the `raw` waveforms:
``` julia
using Unitful, LegendDataManagement, Plots

l200 = LegendData(:l200)

ts = 1.6785791257987175e9u"s"

ch = ChannelId(1104000)

plot(l200, ts, ch)
```

- `plot_tier`: The data tier to be plotted. Default is `DataTier(:raw)`.
- `plot_waveform`: All waveforms to be plotted from the data. Default is `[:waveform_presummed]` which plots the presummed waveform.
- `show_unixtime`: If set to `true`, use unix time instead of the datetime in the title. Default is `false`.

If the channel is not given, the recipe automtically searches for the correct event in the data.
``` julia
ts = 1.6785791257987175e9u"s"

plot(l200, ts)
```
In case of a `cal` event, only the HPGe channel with that event is plotted. In case of a `phy` event, all waveforms of the full HPGe and SiPM systems are plotted. 
The following additional keywords arguments can be set (the `plot_waveform` kwarg is replaced by the `system` kwarg here):
- `system`: The system and the waveforms to be plotted for each system. Default is `Dict{Symbol, Vector{Symbol}}([:geds, :spms] .=> [[:waveform_presummed], [:waveform_bit_drop]])`
- `only_processable`: If set to `true`, only processable channels are plotted. Default is `true`.

## `LegendHDF5IO` extension

LegendDataManagment provides an extension for [LegendHDF5IO](https://github.com/legend-exp/LegendHDF5IO.jl).
This makes it possible to directly load LEGEND data from HDF5 files via the `read_ldata` function. The extension is automatically loaded when both packages are loaded. 
Example (requires a `$LEGEND_DATA_CONFIG` environment variable pointing to a legend data-config file):
    
```julia
using LegendDataManagement, LegendHDF5IO
l200 = LegendData(:l200)
filekeys = search_disk(FileKey, l200.tier[:jldsp, :cal, :p03, :r000])

chinfo = channelinfo(l200, (:p03, :r000, :cal); system=:geds, only_processable=true)

ch = chinfo[1].channel

dsp = read_ldata(l200, :jldsp, first(filekeys), ch)
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, ch)
dsp = read_ldata((:e_cusp, :e_trap, :blmean, :blslope), l200, :jldsp, :cal, :p03, :r000, ch)
```
`read_ldata` automitcally loads LEGEND data for a specific `DataTier` and data selection like e.g. a `FileKey` or a run-selection based for a given `ChannelId`. The `search_disk` function allows the user to search for available `DataTier` and `FileKey` on disk. The first argument can be either a selection of keys in form of a `NTuple` of `Symbol` or a [PropertyFunction](https://github.com/oschulz/PropertyFunctions.jl/tree/main) which will be applied during loading. 
It is also possible to load whole a `DataPartition` or `DataPeriod` for a given `ChannelId` ch:
```julia
dsp = read_ldata(l200, :jldsp, :cal, DataPartition(1), ch)
dsp = read_ldata(l200, :jldsp, :cal, DataPeriod(3), ch)
```
In additon, it is possible to load a random selection of `n_evts` events randomly selected from each loaded file:
```julia
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, ch; n_evts=1000)
```
For simplicity, the ch can also be given as a `DetectorID` which will be converted internally to a `ChannelId`:
```julia
det = chinfo[1].detector
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, det)
```
In case, a `ChannelId` is missing in a file, the function will throw an `ArgumentError`. To avoid this and return `nothing` instead, you can use the `ignore_missing` keyword argument.

The data can be filtered by a `filterby` keyword argument which is a [PropertyFunction](https://github.com/oschulz/PropertyFunctions.jl/tree/main) applied to each chunk of loaded data:
```julia
dsp = read_ldata(l200, :jldsp, :cal, :p03, :r000, ch; filterby=@pf($e_trap > 0.0))
```
This will only load data where the `e_trap` property is greater than 0.

It is possible to read in multiple files in parallel using the `Distributed` functionalities from within a session. You can activate parallel read with the `parallel` kwarg.
``` julia
dsp = read_ldata(l200, :jldsp, :cal, DataPeriod(3), ch)
dsp = read_ldata(l200, :jldsp, :cal, DataPeriod(3), ch; parallel=true)
```
However, it is necessary that a worker allocation was already performed and the `LegendDataManagement` as well as `LegendHDF5IO` package is loaded on all workers, e.g. with
``` julia
using Distributed
addprocs(4)
@everywhere using LegendDataManagement, LegendHDF5IO
```
In addition, the `wpool`kwarg allows to parse a custome `WorkerPool` for more sophisticated load patterns.

## `SolidStateDetectors` extension

LegendDataManagment provides an extension for [SolidStateDetectors](https://github.com/JuliaPhysics/SolidStateDetectors.jl). This makes it possible to create `SolidStateDetector` and `Simulation` instances from LEGEND metadata. The default drift model used when creating a detector/simulation through LegendDataManagment is ADLChargeDriftModel2016.

Example (requires a `$LEGEND_DATA_CONFIG` environment variable pointing to a legend data-config file):

```julia
using LegendDataManagement, SolidStateDetectors, Plots
det = SolidStateDetector(LegendData(:l200), :V99000A)
plot(det)
```

`st = :slice` keyword can be passed to the `plot` to plot a 2D slice of the detector. Using the previous constructor looks up the diode and crystal metadata files. This can also be done manually with the following constructor -- which can also be used directly (no `$LEGEND_DATA_CONFIG` required):

```julia
det = SolidStateDetector(LegendData, "V99000A.yaml", "V99000.yaml")
```
In cases where multiple values (or none) are available in the metadata, the detector is configured using the following priority:
- n⁺ contact thickness: 0νββ analysis value (if available) → manufacturer's value (if available) → default value. Can also be overridden with `n_thickness` keyword.
- Operational Voltage: l200 characterization value (if available) → manufacturer's value (if available) → default value. Can also be overridden with `operational_voltage` keyword.
- Impurity profile: model in crystal metadata (if available) → constant value of 0

To create a SolidStateDetectors `Simulation` or `SolidStateDetector` LegendDataManagement creates a SolidStateDetectors config. Although this process is entirely internal, and does not require any files to be written to disk, the user can choose to do so by using the `ssd_config_filename` keyword (e.g. `ssd_config_filename = "../V99000A_ssd_config.yaml"`). If set, a YAML file will be written as specified. This file which can then be modified and used by SolidStateDetectors independently of LegendDataManagement (e.g. `Simulation{T}("../V99000A_ssd_config.yaml")`). By default `ssd_config_filename` is set to `missing`. 

!!! note
    Simulating a LEGEND detector from a modified `ssd_config` can lead to errors and divergent behavior from LEGEND defaults. Please read the [SolidStateDetectors documentation](https://juliaphysics.github.io/SolidStateDetectors.jl/stable/man/config_files/) before proceeding.

In addition, when creating a `Simulation`, all simulation functions in SolidStateDetectors.jl can be applied. As usual, all fields stored in the `Simulation` can be written and read using `LegendHDF5IO`:

```julia
using LegendDataManagement
using SolidStateDetectors
using Unitful

T=Float32

sim = Simulation{T}(LegendData(:l200), :V99000A, HPGeEnvironment("LAr", 87u"K"), n_thickness = 0.7u"mm", operational_voltage = 4u"kV")
simulate!(sim) # calculate electric field and weighting potentials

using LegendHDF5IO
ssd_write("V99000A.lh5", sim)
sim_in = ssd_read("V99000A.lh5", Simulation)
```


The following code will generate an overview plot of every 5th LEGEND detector (requires the actual LEGEND metadata instead of the metadata in legend-testdata):

```julia
using LegendDataManagement, SolidStateDetectors, Plots
l200 = LegendData(:l200)
detnames = propertynames(l200.metadata.hardware.detectors.germanium.diodes)
plot(
    plot.(SolidStateDetector.(Ref(l200), detnames[1:5:120]))...,
    layout = (3,8), lw = 0.05, legend = false, grid = false, showaxis = false,
    xlims = (-0.05,0.05), ylims = (-0.05,0.05), zlims = (0,0.1), size = (4000,1500)
)
```
