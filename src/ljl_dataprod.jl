# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    dataprod_config(data::LegendData)

Get the Julia data production configuration for `data`.

Use `dataprod_config(data)(valsel::AnyValiditySelection)` to also set the
time/category validity selection for the configuration.

Examples:

```julia
l200 = LegendData(:l200)
filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
dataprod_config(l200)(filekey)
```

or

```
l200 = LegendData(:l200)
vsel = ValiditySelection("20221226T200846Z", :cal)
dataprod_config(l200)(vsel)
```
"""
function dataprod_config(data::LegendData)
    metadata = data.metadata
    # ToDo: Remove fallback to `data.dataprod` when no longer required.
    dataprod_metadata = hasproperty(metadata, :jldataprod) ? metadata.jldataprod : metadata.dataprod
    dataprod_metadata.config
end
export dataprod_config


"""
    energy_cal_config(data::LegendData, sel::AnyValiditySelection, det::DetectorIdLike)

Get the energy calibration configuration.
"""
function energy_cal_config(data::LegendData, sel::AnyValiditySelection, detector::DetectorIdLike)
    det = DetectorId(detector)
    prodcfg = dataprod_config(data)
    ecfg = prodcfg.cal.energy(sel)
    if haskey(ecfg, det)
        merge(ecfg.default, ecfg[det])
    else
        ecfg.default
    end
end
export energy_cal_config


"""
    ecal_peak_windows(ecal_cfg::PropDict)

Get a dictionary of gamma peak windows to be used for energy calibrations.

Returns a `Dict{Symbol,<:AbstractInterval{<:Real}}`.

Usage:

```julia
ecal_peak_windows(energy_cal_config(data, sel, detector))
```
"""
function ecal_peak_windows(ecal_cfg::PropDict)
    labels::Vector{Symbol} = Symbol.(ecal_cfg.th228_names)
    linepos::Vector{Float64} = ecal_cfg.th228_lines
    left_size::Vector{Float64} = ecal_cfg.left_window_sizes
    right_size::Vector{Float64} = ecal_cfg.right_window_sizes
    Dict([label => ClosedInterval(peak-lsz, peak+rsz) for (label, peak, lsz, rsz) in zip(labels, linepos, left_size, right_size)])
end
export ecal_peak_windows
