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

LegendDataManagement provides an extension for [LegendHDF5IO](https://github.com/legend-exp/LegendHDF5IO.jl) that exposes `read_ldata` for loading LEGEND data from HDF5 files. The extension auto-loads when both packages are loaded. All examples assume `$LEGEND_DATA_CONFIG` is set.

```julia
using LegendDataManagement, LegendHDF5IO, PropertyFunctions
using Unitful: @u_str

l200 = LegendData(:l200)
fk   = first(search_disk(FileKey, l200.tier[:jldsp, :cal, :p03, :r000]))
det  = first(filter(c -> c.system == :geds && c.processable, channelinfo(l200, fk))).detector
```

### Selecting columns

Three equivalent forms (all return a `Table`):

```julia
read_ldata(l200, :jldsp, fk, det)                                       # all columns
read_ldata((:e_cusp, :timestamp), l200, (:jldsp, fk, det))              # tuple of names
read_ldata(@pf((; $e_cusp, $timestamp)), l200, (:jldsp, fk, det))       # PropSel via @pf
```

The PropSel form enables a fast path that only loads the requested leaf columns from disk.

### Per-detector slicing across tiers

`read_ldata(_, l200, (tier, fk, det))` works for raw, jldsp, jlhit, jlpls, jlpeaks, and the event tiers jlevt, jlskm, jlpmt. The detector can be a GED, SiPM, or PMT — the system is picked up from `channelinfo`. Without a detector, an event-tier read returns the native nested LH5 layout:

```julia
read_ldata(l200, :raw,   fk, det)        # raw waveforms for one detector
read_ldata(l200, :jlpmt, fk, pmt_det)    # PMT event-tier table
evt = read_ldata(l200, :jlevt, fk)       # nested: evt.aux.pulser.aux_trig, evt.geds.is_valid_qc, …
```

For the per-det event-tier read the columns are flat-prefixed (`geds_e_cusp_ctc_cal`, `geds_trig_e_cusp_ctc_cal`, `spms_trig_max_cal`, `aux_pulser_aux_trig`, `ged_spm_is_valid_lar`, …). Per-trigger and per-det-list VoVs are unwrapped at the detector's per-event slot, and only events where the detector triggered are kept.

### Filtering during the read

`filterby` is a `PropertyFunction` applied row-wise to the loaded table; `missing` predicate values are dropped (treated as `false`):

```julia
read_ldata((:geds_e_cusp_ctc_cal,), l200, (:jlevt, fk, det);
           filterby = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV")
```

### Cross-tier filter

The `filtertier` kwarg lets a per-detector read use a filter from a different tier. Two modes, both keyed by detector:

- **event tier → raw / jldsp** — uses the per-det `*_dataidx` column to slice (1-based, same for raw and jldsp). Use this to pull, e.g., raw waveforms only for events that pass an event-level QC cut.
- **raw ↔ jldsp** — uses the 1:1 row alignment between raw and jldsp for a given detector and applies a Bool row-mask. Works for `phy` *and* `cal` (no event tier needed).

```julia
# Raw waveforms of phy events that pass an event-tier QC + energy cut
read_ldata(@pf((; $waveform_presummed, $timestamp)),
           l200, (:raw, fk, det);
           filterby   = @pf $geds_is_valid_qc && $geds_trig_e_cusp_ctc_cal > 1500u"keV",
           filtertier = :jlevt)

# Cal raw waveforms for events with high reconstructed dsp energy (no jlevt for cal)
read_ldata((:waveform_presummed,), l200, (:raw, fk_cal, det);
           filterby   = @pf $e_cusp > 1000,
           filtertier = :jldsp)
```

### Run / period / partition selection

```julia
read_ldata(l200, :jldsp, :cal, :p03, :r000, det)            # one run
read_ldata(l200, :jldsp, :cal, DataPeriod(3),    det)       # one period
read_ldata(l200, :jldsp, :cal, DataPartition(1), det)       # one partition
```

A `Vector{FileKey}` is also accepted as the second selector. Both `parallel=true` and a `wpool::WorkerPool` are supported for `Distributed` reads (workers must have `LegendDataManagement` and `LegendHDF5IO` loaded).

### Other kwargs

- `subgroup=:dataQC` — descend into a per-det sub-group (e.g. `jlhit/<det>/dataQC`).
- `n_evts=1000` — random sample of `n_evts` rows per file.
- `ignore_missing=true` — return `nothing` instead of throwing when a detector is missing in a file.

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
