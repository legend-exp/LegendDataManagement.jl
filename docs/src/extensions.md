# Extensions

## `SolidStateDetectors` extension

LegendDataManagment provides an extension for [SolidStateDetectors](https://github.com/JuliaPhysics/SolidStateDetectors.jl). This makes it possible to create `SolidStateDetector` instances from LEGEND metadata.

Example (requires a `$LEGEND_CONFIG` environment variable pointing to a legend data-config file):

```julia
using LegendDataManagement, SolidStateDetectors, Plots
det = SolidStateDetector(LegendData(:l200), :V99000A)
plot(det)
```

A detector can also be constructed using the filename of the LEGEND metadata detector-datasheet JSON file (no `$LEGEND_CONFIG` required):

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
