# Extensions

## `LegendHDF5IO` extension

LegendDataManagment provides an extension for [LegendHDF5IO](https://github.com/legend-exp/LegendHDF5IO.jl).
This makes it possible to directly load LEGEND data from HDF5 files via the `read_ldata` function. The extension is automatically loaded when both packages are loaded. 
Example (requires a `$LEGEND_DATA_CONFIG` environment variable pointing to a legend data-config file):
    
```julia
using LegendDataManagement, LegendHDF5IO
l200 = LegendData(:l200)
filekeys = search_dsik(FileKey, l200.tier[:jldsp, :cal, :p03, :r000])

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

## `SolidStateDetectors` extension

LegendDataManagment provides an extension for [SolidStateDetectors](https://github.com/JuliaPhysics/SolidStateDetectors.jl). This makes it possible to create `SolidStateDetector` instances from LEGEND metadata.

Example (requires a `$LEGEND_DATA_CONFIG` environment variable pointing to a legend data-config file):

```julia
using LegendDataManagement, SolidStateDetectors, Plots
det = SolidStateDetector(LegendData(:l200), :V99000A)
plot(det)
```

A detector can also be constructed using the filename of the LEGEND metadata detector-datasheet JSON file (no `$LEGEND_DATA_CONFIG` required):

```julia
det = SolidStateDetector(LegendData, "V99000A.json")
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
