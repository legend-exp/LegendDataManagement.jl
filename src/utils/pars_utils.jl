"""
    create_pars(pd::PropDict, result) -> PropDict

Creates a `PropDict` of parameters from a result of the parallel processing.

`result` is a collection of pairs `(chinfo, res)`. For each entry that has
the property `res.result` and was processed, the content of `res.result` is
merged into `pd[chinfo.detector]`. Returns `pd`, which is modified.
"""
function create_pars(pd::PropDict, result)
    for (chinfo_ch, res_ch) in result
        if haskey(res_ch, :result)
            if  res_ch.processed isa Dict || res_ch.processed
                det = chinfo_ch.detector
                pd_det = ifelse(haskey(pd, det), pd[det], PropDict())
                pd_det = nt2pd(pd_det, res_ch.result)
                pd[det] = pd_det
            end
        end
    end
    pd
end
export create_pars

# convert NamedTuple to PropDict
function nt2pd(pd::PropDict, nt::Union{NamedTuple, Dict, PropDict})
    for k in keys(nt)
        if nt[k] isa NamedTuple || nt[k] isa Dict || nt[k] isa PropDict
            pd[k] = if !(haskey(pd, k)) || isnothing(pd[k]) PropDict() else pd[k] end
            nt2pd(pd[k], nt[k])
        else
            pd[k] = nt[k]
        end
    end
    pd
end

"""
    writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Union{String, Vector{String}}; category::DataCategoryLike = :all, mode::AbstractString = "reset")
    writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, rsel::Tuple{DataPeriod, DataRun}; category::DataCategoryLike = :all, mode::AbstractString = "reset")
    writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, part::DataPartition; category::DataCategoryLike = :all, mode::AbstractString = "reset")
    writevalidity(props_db::LegendDataManagement.MaybePropsDB, validity_with_flag::NamedTuple{(:result, :skipped)}; category::DataCategoryLike = :all, impl::Symbol = :full)

Writes validity information into the file `validity.yaml` in the directory of
`props_db`.

The file is created if it does not exist. An entry with the time of `filekey`
is added; an existing entry with the same time and mode is replaced. All
entries are sorted by time.

* With `apply` (a file name or a vector of file names), the entry applies these files with the given `mode` (`"reset"`, `"append"`, `"remove"` or `"replace"`; `"replace"` needs a pair of the file to remove and the file to add).
* A `(DataPeriod, DataRun)` is converted to the file name `<period>/<run>.yaml` and a `DataPartition` to `<partition>.yaml`.
* With a `NamedTuple` `(result, skipped)`, as returned by [`create_validity`](@ref), a sequence of entries is written for the detectors and partitions in `result`. The implementation `impl = :full` resets the validity at the earliest first partition (`a`) of the detectors and replaces, or removes and appends, the later partitions of each detector. The implementation `impl = :diff` writes the differences of the validity entries between consecutive times as `append`, `remove` or `replace` entries, and a `reset` if more than ten entries change. If `skipped` is `true` the first entry is appended instead of reset. Other values of `impl` throw an `ArgumentError`.

## Keywords
* `category::DataCategoryLike = :all`: Data category the entry is valid for.
* `mode::AbstractString = "reset"`: How `apply` changes the files that are valid. Not available with `(result, skipped)`.
* `impl::Symbol = :full`: Implementation for `(result, skipped)`, `:full` or `:diff`.

See also [`create_validity`](@ref).
"""
function writevalidity end
export writevalidity
function writevalidity(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Vector{String}; category::DataCategoryLike=:all, mode::AbstractString = "reset")
    Distributed.remotecall_fetch(_writevalidity_impl, 1, props_db, filekey, apply; category, mode, write_from_master= (Distributed.myid() == 1))
end
const _writevalidity_lock = ReentrantLock()
function _writevalidity_impl(props_db::LegendDataManagement.MaybePropsDB, filekey::FileKey, apply::Vector{String};
    category::DataCategoryLike = :all, mode::AbstractString = "reset", write_from_master::Bool = true)

    @lock _writevalidity_lock begin
        # locate & create file if missing
        dst = joinpath(data_path(props_db), "validity.yaml")
        mkpath(dirname(dst))
        if !isfile(dst)
            open(dst, "w") do io
                println(io, "[]")
            end
        end

        # load or initialize the vector of entries
        raw     = YAML.load_file(dst, dicttype = OrderedDict{String, Any})
        entries::Vector{OrderedDict{String,Any}} = raw isa Vector ? raw : OrderedDict{String,Any}[]
        ts = string(filekey.time)
        # drop any previous entry with the same timestamp
        filter!(e -> !(e["valid_from"] == ts && e["mode"] == mode), entries)
        # build an OrderedDict containing the validity to be saved
        push!(entries, OrderedDict{String,Any}(
            "valid_from" => ts,
            "apply" => apply,
            "category" => category,
            "mode" => mode
        ))
        
        write_from_master && @info "Write validity for $ts"
        # sort the entries by timestamp
        sort!(entries, by = e -> e["valid_from"])
        
        # create a new file with the same filename with updated entries
        ParallelProcessingTools.write_files(dst; mode=CreateOrReplace()) do tmpfile
            open(tmpfile, "w") do io
                YAML.write(io, entries)
            end
        end
        return nothing
    end
end
writevalidity(props_db, filekey, apply::String; kwargs...) = writevalidity(props_db, filekey, [apply]; kwargs...)
writevalidity(props_db, filekey, rsel::Tuple{DataPeriod, DataRun}; kwargs...) = writevalidity(props_db, filekey, "$(first(rsel))/$(last(rsel)).yaml"; kwargs...)
writevalidity(props_db, filekey, part::DataPartition; kwargs...) = writevalidity(props_db, filekey, "$(part).yaml"; kwargs...)
function writevalidity(
    props_db::LegendDataManagement.MaybePropsDB,
    validity_with_flag::NamedTuple{(:result, :skipped)};
    category::DataCategoryLike = :all,
    impl::Symbol = :full # :full or :diff
)
    if impl == :full
        # Ensures each detector has a validity entry for every run, even if not grouped (useful e.g. for AC runs).
        return writevalidity_full(props_db, validity_with_flag; category=category)
    elseif impl == :diff
        # Detects differences in validity entries per timestamp and writes each change into validity.yaml.
        return writevalidity_diff(props_db, validity_with_flag; category=category)
    else
        throw(ArgumentError("Unknown implementation: $impl"))
    end
end

function writevalidity_full(
    props_db::LegendDataManagement.MaybePropsDB,
    validity_with_flag::NamedTuple{(:result, :skipped)};
    category::DataCategoryLike = :all
)
    validity = validity_with_flag.result

    # 1) only keep earliest timestamp per unique validity string
    validity_first_ts = Dict{String, Timestamp}()
    validity_filekey  = Dict{Timestamp, FileKey}()
    for row in validity
        ts = row.filekey.time
        val = row.validity
        if !haskey(validity_first_ts, val) || ts < validity_first_ts[val]
            validity_first_ts[val] = ts
            validity_filekey[ts] = row.filekey
        end
    end

    # 2) collect first 'a' partitions per detector and find global reset timestamp
    det_to_a_validity = Dict{String, String}()      # detector => "...a.yaml"
    det_to_a_ts       = Dict{String, Timestamp}()   # detector => first 'a' ts
    for (val, ts) in validity_first_ts
        if endswith(val, "a.yaml")
            det = split(val, '/')[1]
            det_to_a_validity[det] = val
            det_to_a_ts[det] = ts
        end
    end
    reset_ts = isempty(det_to_a_ts) ? nothing : minimum(values(det_to_a_ts))

    # 3) group non-'a' transitions as remove(prev) + append(new)
    ts_to_adds    = Dict{Timestamp, Vector{String}}()
    ts_to_removes = Dict{Timestamp, Vector{String}}()

    for (val, ts) in validity_first_ts
        det, suffix = split(val, '/')
        if endswith(val, "a.yaml")
            continue
        end
        # parse partition (strip ".yaml"), compute predecessor (same number, previous set)
        part      = DataPartition(replace(suffix, ".yaml" => ""))
        prev_char = Char(first(string(part.set))) - 1
        prev_part = DataPartition(part.no, Symbol(prev_char), part.cat)
        prev_val  = string(det, "/", prev_part, ".yaml")

        push!(get!(ts_to_adds, ts, String[]), val)
        push!(get!(ts_to_removes, ts, String[]), prev_val)
    end

    # 4) emit writes in chronological order, grouping per timestamp (≤1 remove, ≤1 append)
    timestamps = sort!(collect(union(keys(ts_to_adds), keys(ts_to_removes),
                                     reset_ts === nothing ? Timestamp[] : [reset_ts])))

    for ts in timestamps
        fk = validity_filekey[ts]
        appends = String[]
        removes = String[]

        # baseline handling at earliest 'a' timestamp
        if ts == reset_ts
            if validity_with_flag.skipped
                append!(appends, values(det_to_a_validity))   # treat as append when skipped=true
            else
                vals = sort!(collect(values(det_to_a_validity)))
                writevalidity(props_db, fk, vals; category=category, mode="reset")
            end
        end

        if haskey(ts_to_removes, ts)
            append!(removes, ts_to_removes[ts])
        end
        if haskey(ts_to_adds, ts)
            append!(appends, ts_to_adds[ts])
        end

        # use "replace" if exactly one remove and one append
        if length(removes) == 1 && length(appends) == 1
            writevalidity(props_db, fk, [removes[1], appends[1]]; category=category, mode="replace")
        else
            if !isempty(removes)
                writevalidity(props_db, fk, sort!(removes); category=category, mode="remove")
            end
            if !isempty(appends)
                writevalidity(props_db, fk, sort!(appends); category=category, mode="append")
            end
        end
    end
end

function writevalidity_diff(
    props_db::LegendDataManagement.MaybePropsDB,
    validity_with_flag::NamedTuple{(:result, :skipped)};
    category::DataCategoryLike = :all
)
    validity = validity_with_flag.result

    # Group by FileKey timestamp
    ts_to_validities = Dict{Timestamp, Set{String}}()
    ts_to_filekey = Dict{Timestamp, FileKey}()

    for row in validity
        ts = row.filekey.time
        val = row.validity
        push!(get!(ts_to_validities, ts, Set{String}()), val)
        ts_to_filekey[ts] = row.filekey
    end

    # Sort timestamps
    timestamps = sort(collect(keys(ts_to_validities)))

    prev_validities = Set{String}()
    first = true

    for ts in timestamps
        current_validities = ts_to_validities[ts]
        fk = ts_to_filekey[ts]

        if first # First timestamp: reset
            mode = validity_with_flag.skipped ? "append" : "reset"
            writevalidity(props_db, fk, sort(collect(current_validities)); category=category, mode=mode)
            prev_validities = deepcopy(current_validities)
            first = false
            continue
        end

        if current_validities == prev_validities # No change -> skip writing validity
            continue
        end

        added = setdiff(current_validities, prev_validities)
        removed = setdiff(prev_validities, current_validities)

        if !isempty(added) && isempty(removed)
            writevalidity(props_db, fk, sort(collect(added)); category=category, mode="append")
        elseif isempty(added) && !isempty(removed)
            writevalidity(props_db, fk, sort(collect(removed)); category=category, mode="remove")
        elseif length(added) == 1 && length(removed) == 1
            writevalidity(props_db, fk, [only(removed), only(added)]; category=category, mode="replace")
        elseif length(added) + length(removed) <= 10
            # split state into small append and remove block instead of using multiple "replace" or big "reset"
            writevalidity(props_db, fk, sort(collect(removed)); category=category, mode="remove")
            writevalidity(props_db, fk, sort(collect(added)); category=category, mode="append")
        else
            # Fallback to reset: write full current state
            writevalidity(props_db, fk, sort(collect(current_validities)); category=category, mode="reset")
        end

        prev_validities = deepcopy(current_validities)
    end
end

"""
    create_validity(result) -> NamedTuple{(:result, :skipped)}

Creates validity information from a result of the parallel processing.

`result` is a collection of pairs `(chinfo, res)`. Returns a `NamedTuple` with
the `StructArray` `result` of the entries `(period, run, filekey, validity)`
that are the `res.validity` of all entries that were not skipped, and
`skipped`, which is `true` if any entry has `res.skipped == true`. The result
can be passed to [`writevalidity`](@ref).
"""
function create_validity(result)
    validity_all = Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}()
    skipped = false
    for (_, res_ch) in result
        skipped |= get(res_ch, :skipped, false)  # Set to true if any channel was skipped
        if haskey(res_ch, :validity) && !get(res_ch, :skipped, false)
            append!(validity_all, res_ch.validity)
        end
    end
    return (result = StructArray(validity_all), skipped = skipped)
end
export create_validity

"""
    get_partitionvalidity(data::LegendData, det::DetectorIdLike, part::DataPartitionLike) -> Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}
    get_partitionvalidity(data::LegendData, ch::ChannelIdLike, det::DetectorIdLike, part::DataPartitionLike, cat::DataCategoryLike = :cal)

Returns the validity entries of a partition for a detector.

The result has one entry for each run of the partition, with the start file
key of the run and the validity file name `<det>/<partition>.yaml`. The method
with the additional channel and category arguments is deprecated; use the
method with `det` and `part`.

See also [`writevalidity`](@ref).
"""
function get_partitionvalidity(data::LegendData, det::DetectorIdLike, part::DataPartitionLike)
    # unpack
    det, part = DetectorId(det), DataPartition(part)
    # get partition validity
    partinfo = partitioninfo(data, det, part)
    Vector{@NamedTuple{period::DataPeriod, run::DataRun, filekey::FileKey, validity::String}}([(period = pinf.period, run = pinf.run, filekey = start_filekey(data, (pinf.period, pinf.run, part.cat)), validity = "$det/$(part).yaml") for pinf in partinfo])
end
export get_partitionvalidity
# dropped unecessaty ch argument, keep old function functionality for now
@deprecate get_partitionvalidity(data::LegendData, ch::ChannelIdLike, det::DetectorIdLike, part::DataPartitionLike, cat::DataCategoryLike=:cal) get_partitionvalidity(data, det, part)

"""
    detector2channel(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, detector::DetectorIdLike)

Returns the [`ChannelId`](@ref) of a detector for the given selection.

`runsel` is a validity selection, a file key or a run `(period, run, category)`,
e.g. `(DataPeriod(3), DataRun(0), :cal)`. The channel is taken from
[`channelinfo`](@ref).

See also [`channel2detector`](@ref).
"""
function detector2channel(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, detector::DetectorIdLike)
    return channelinfo(data, runsel, detector).channel
end
export detector2channel

"""
    channel2detector(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, channel::ChannelIdLike)

Returns the [`DetectorId`](@ref) of a channel for the given selection.

`runsel` is a validity selection, a file key or a run `(period, run, category)`,
e.g. `(DataPeriod(3), DataRun(0), :cal)`. The detector is taken from
[`channelinfo`](@ref).

See also [`detector2channel`](@ref).
"""
function channel2detector(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike}, channel::ChannelIdLike)
    return channelinfo(data, runsel, channel).detector
end
export channel2detector

"""
    detector_type(data::LegendData, det::DetectorIdLike)

Returns the type of a germanium detector, e.g. `:icpc`, as a `Symbol`.

The type is read from the diode metadata of `det` in
`data.metadata.hardware.detectors.germanium.diodes`.
"""
function detector_type(data::LegendData, det::DetectorIdLike)
    det_type = Symbol(data.metadata.hardware.detectors.germanium.diodes[det].type)
    return det_type
end
export detector_type

"""
    data_starttime(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike})

Returns the start time of a run as a `DateTime`.

`runsel` is a file key or a run `(period, run, category)`, e.g.
`(DataPeriod(3), DataRun(0), :cal)`. The time is that of the start file
key of the run (see [`start_filekey`](@ref)).
"""
function data_starttime(data::LegendData, runsel::Union{AnyValiditySelection, RunCategorySelLike})
    filekey = start_filekey(data, runsel)
    startdate = DateTime(filekey.time)
    return startdate
end
export data_starttime
