# Exposure and Livetime

The livetime of a run is stored in the run information (see [Channel, Run and Partition Information](@ref)). The exposure of a detector is the product of its mass and the livetime, summed over runs. Both are `Unitful` quantities.

```@setup ldm
include(joinpath(@__DIR__, "..", "testdata_setup.jl"))
```

```@example ldm
using Unitful, PropertyFunctions
```

## Livetime

[`livetime`](@ref) returns the livetime of a run `(period, run, category)` in seconds. The value is `NaN` seconds if the metadata has none, as for the calibration runs of the test production.

```@example ldm
livetime(l200, (:p02, :r006, :phy))
```

The `livetime` column of the category tables of [`runinfo`](@ref) holds the values of several runs. They add and convert like any `Unitful` quantity:

```@example ldm
livetimes = runinfo(l200, :p02).phy.livetime
total = sum(livetimes)
```

```@example ldm
uconvert(u"d", total)
```

```@example ldm
ustrip(u"s", total)
```

## Exposure

[`get_exposure`](@ref) calculates the exposure of a detector as

```math
\sum_{\text{runs}} m_\text{det} \, t_\text{live}
```

in `kg * yr`. The detector mass ``m_\text{det}`` is taken from the channel information at the start file key of each run (the column `mass` of [`channelinfo`](@ref) with `extended = true`), the livetime ``t_\text{live}`` from the run information. The mass is the total detector mass, not the active mass. `yr` is the Julian year of Unitful (365.25 days).

The runs are selected by

* a period and a run: `get_exposure(data, det, period, run)`,
* a period: `get_exposure(data, det, period)`,
* a [`DataPartition`](@ref) of the detector: `get_exposure(data, det, partition)`,
* a name that is converted to a partition or a period, like `:p02` or `:phypartition001a`,
* a table of run information such as returned by [`runinfo`](@ref).

```@example ldm
get_exposure(l200, :V99000A, DataPeriod(2), DataRun(6))
```

```@example ldm
get_exposure(l200, :V99000A, DataPeriod(2))
```

```@example ldm
get_exposure(l200, :V99000A, :p02) == get_exposure(l200, :V99000A, runinfo(l200, :p02))
```

The result is a quantity of Unitful, so it converts to other units:

```@example ldm
uconvert(u"kg * d", get_exposure(l200, :V99000A, :p02))
```

```@example ldm
ustrip(u"kg * yr", get_exposure(l200, :V99000A, :p02))
```

The mass of the detector `V99000A` in the test metadata is 2.5 kg and its `phy` livetime in period `p02` is 80000 s in total, so the exposure is 200000 kg s.

### Partitions

For a [`DataPartition`](@ref) the exposure is calculated over the runs of that partition for the detector, in the category of the partition. The exposure is zero for a partition that does not exist for the detector. A category without livetime in the metadata gives `NaN`.

```@example ldm
get_exposure(l200, :V99000A, DataPartition(:phypartition001a))
```

```@example ldm
get_exposure(l200, :V99000A, DataPartition(:phypartition099a))
```

### Several Detectors

A vector of detectors gives the sum of their exposures. Broadcasting over the detectors gives one value per detector.

```@example ldm
get_exposure(l200, [:V99000A, :B99000A], :p02)
```

```@example ldm
get_exposure.(l200, [:V99000A, :B99000A], :p02)
```

### Keywords

* `cat`: Data category of the runs, default `:phy`. For a partition, the category of the partition is used. `get_exposure` throws an `ArgumentError` if the category is not a column of the run information.
* `check_pf`: A property function that is applied to the channel information row of the detector at the start key of each run. A run counts only if it returns `true`. The default is `@pf $detector == DetectorId(det)`, which is always true for the selected detector. Any property function of the columns of [`channelinfo`](@ref) works, see [Property Functions and Expressions](@ref).
* `is_analysis_run`: Accepted for the method with a table of run information, but it does not change the result. The runs of the table are all counted; select them beforehand, e.g. with `filterby(@pf $phy.is_analysis_run)`.

The following counts only the runs in which the detector has the usability `:on`:

```@example ldm
get_exposure(l200, :V99000A, :p02; check_pf = @pf $usability == :on)
```

```@example ldm
get_exposure(l200, :B99000A, :p02; check_pf = @pf $usability == :on)
```

A table of run information lets the runs be selected before the calculation:

```@example ldm
rinfo = runinfo(l200, :p02) |> filterby(@pf $phy.is_analysis_run && $run >= DataRun(4))
get_exposure(l200, :V99000A, rinfo)
```

## Mass and Active Volume

The channel information with `extended = true` holds the quantities the exposure is built from, and the active volume of each detector. The active volume is calculated from the detector geometry in `hardware/detectors/germanium/diodes` and the full charge collection depth (FCCD) `fccd`, the thickness of the dead layer. It depends on the detector type (`bege`, `icpc`, `coax` or `ppc`) and takes the extra geometry features of special detectors into account. It is not used by [`get_exposure`](@ref), which uses the total mass.

Here, `mfk` is a file key of the toy production in the test metadata (period 2, run 6, category `cal`).

```@example ldm
chinfo = channelinfo(l200, mfk; extended = true, verbose = false)
(chinfo.detector, chinfo.det_type)
```

```@example ldm
chinfo.mass
```

```@example ldm
chinfo.fccd
```

```@example ldm
chinfo.total_volume
```

```@example ldm
chinfo.active_volume
```

The columns have units, so they convert with `uconvert`; for example, the FCCD in micrometers:

```@example ldm
uconvert.(u"μm", chinfo.fccd)
```
