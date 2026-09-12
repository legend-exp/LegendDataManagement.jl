module LegendDataManagementLegendHDF5IOExt

using LegendDataManagement
LegendDataManagement._lh5_ext_loaded(::Val{true}) = true
using LegendDataManagement: RunCategorySelLike
using LegendHDF5IO
using LegendDataTypes: fast_flatten
using StructArrays
using TypedTables, PropertyFunctions
using Distributed, ProgressMeter


const AbstractDataSelectorLike = Union{AbstractString, Symbol, DataTierLike, DataCategoryLike, DataPeriodLike, DataRunLike, DataPartitionLike, DetectorIdLike}
# `nothing` reads every channel of the tier; a channel of a raw tier need not be a detector.
const DetectorSel = Union{DetectorId, Nothing}
const ChannelSel = Union{DetectorId, AbstractString, Nothing}
# What an element of a selection may be: a selector, a filekey, a run table, or a vector.
const RSelLike = Union{AbstractDataSelectorLike, FileKey, AbstractVector, Nothing}
const PossibleDataSelectors = [DataTier, DataCategory, DataPeriod, DataRun, DataPartition, DetectorId]


const dataselector_bytypes = Dict{Type, String}()

LegendHDF5IO.datatype_to_string(::Type{<:T}) where {T <: LegendDataManagement.DataSelector} = 
    dataselector_bytypes[T]

function LegendHDF5IO._array_type(::Type{Array{T, N}}
    ) where {T <: LegendDataManagement.DataSelector, N}
    
    AbstractArray{T, N}
end

# write LegendDataManagement.DataSelector
function LegendHDF5IO.create_entry(parent::LHDataStore, name::AbstractString, 
    data::T; kwargs...) where {T <:LegendDataManagement.DataSelector}
    
    LegendHDF5IO.create_entry(parent, name, string(data); kwargs...)
    LegendHDF5IO.setdatatype!(parent.data_store[name], T)
    nothing
end

# write DetectorId - use UInt32 encoding
function LegendHDF5IO.create_entry(parent::LHDataStore, name::AbstractString, 
    data::DetectorId; kwargs...)
    
    LegendHDF5IO.create_entry(parent, name, UInt32(data); kwargs...)
    LegendHDF5IO.setdatatype!(parent.data_store[name], DetectorId)
    nothing
end

# write AbstractArray{<:LegendDataManagement.DataSelector}
function LegendHDF5IO.create_entry(parent::LHDataStore, name::AbstractString, 
    data::T; kwargs...) where {T <:AbstractArray{<:LegendDataManagement.DataSelector}}
    
    LegendHDF5IO.create_entry(parent, name, string.(data); kwargs...)
    LegendHDF5IO.setdatatype!(parent.data_store[name], T)
    nothing
end

# write AbstractArray{<:DetectorId} - use UInt32 encoding
function LegendHDF5IO.create_entry(parent::LHDataStore, name::AbstractString, 
    data::AbstractArray{<:DetectorId}; kwargs...)
    
    LegendHDF5IO.create_entry(parent, name, UInt32.(data); kwargs...)
    LegendHDF5IO.setdatatype!(parent.data_store[name], typeof(data))
    nothing
end

LegendHDF5IO.LH5Array(ds::LegendHDF5IO.HDF5.Dataset, ::Type{<:T}
    ) where {T <: LegendDataManagement.DataSelector} = begin
    
    s = read(ds)
    T(s)
end

# Read DetectorId - support both string and UInt32 encoding
function LegendHDF5IO.LH5Array(ds::LegendHDF5IO.HDF5.Dataset, ::Type{<:DetectorId})
    data = read(ds)
    if data isa AbstractString
        DetectorId(data)
    elseif data isa Integer
        DetectorId(data)
    else
        throw(ArgumentError("Cannot read DetectorId from data of type $(typeof(data))"))
    end
end

function LegendHDF5IO.LH5Array(ds::LegendHDF5IO.HDF5.Dataset, 
    ::Type{<:AbstractArray{<:T, N}}) where {T <: LegendDataManagement.DataSelector, N}

    s = read(ds)
    T.(s)
end

# Read array of DetectorId - support both string and UInt32 encoding
function LegendHDF5IO.LH5Array(ds::LegendHDF5IO.HDF5.Dataset, 
    ::Type{<:AbstractArray{<:DetectorId, N}}) where {N}
    
    data = read(ds)
    if eltype(data) <: AbstractString
        DetectorId.(data)
    elseif eltype(data) <: Integer
        DetectorId.(data)
    else
        throw(ArgumentError("Cannot read DetectorId array from data of element type $(eltype(data))"))
    end
end

function __init__()
    function extend_datatype_dict(::Type{T}, key::String
        ) where {T <: LegendDataManagement.DataSelector}

        LegendHDF5IO._datatype_dict[key] = T
        dataselector_bytypes[T] = key
    end

    (@isdefined ExpSetup) && extend_datatype_dict(ExpSetup, "expsetup")
    (@isdefined DataTier) && extend_datatype_dict(DataTier, "datatier")
    (@isdefined DataRun) && extend_datatype_dict(DataRun, "datarun")
    (@isdefined DataPeriod) && extend_datatype_dict(DataPeriod, "dataperiod")
    (@isdefined DataCategory) && extend_datatype_dict(DataCategory, "datacategory")
    (@isdefined Timestamp) && extend_datatype_dict(Timestamp, "timestamp")
    (@isdefined FileKey) && extend_datatype_dict(FileKey, "filekey")
    (@isdefined ChannelId) && extend_datatype_dict(ChannelId, "channelid")
    (@isdefined DetectorId) && extend_datatype_dict(DetectorId, "detectorid")
    (@isdefined DataPartition) && extend_datatype_dict(DataPartition, "datapartition")
end

# Paths are resolved and stat'ed one at a time: file metadata lookups are expensive on
# the parallel filesystems holding the production data.
function _lh5_data_open(f::Function, data::LegendData, tier::DataTier, filekey::FileKey, det::ChannelSel, mode::AbstractString="r")
    t = tier
    det_filename = isnothing(det) ? nothing : data.tier[t, filekey, det]
    if !isnothing(det_filename) && isfile(det_filename)
        @debug "Read from $(basename(det_filename))"
        return LegendHDF5IO.lh5open(f, det_filename, mode)
    end
    filename = data.tier[t, filekey]
    isfile(filename) && return LegendHDF5IO.lh5open(f, filename, mode)
    throw(ArgumentError(isnothing(det_filename) ? "$(basename(filename)) not found" :
        "Neither $(basename(filename)) nor $(basename(det_filename)) found"))
end

_skipnothingmissing(xv::AbstractVector) = [x for x in skipmissing(xv) if !isnothing(x)]
lflatten(x) = fast_flatten(collect(_skipnothingmissing(x)))
# Each file holds the detectors that triggered in it, so the flatten is key-wise over every
# key any of them has: a file without that key simply contributes no rows.
function lflatten(nts::AbstractVector{<:NamedTuple})
    v = collect(_skipnothingmissing(nts))
    ks = Tuple(unique(Iterators.flatten(map(keys, v))))
    NamedTuple{ks}(map(k -> lflatten([x[k] for x in v if haskey(x, k)]), ks))
end

# The source paths are a Tuple type of PPath types (PropertyFunctions 0.3):
_ppath_path(::Type{PropertyFunctions.PPath{path}}) where path = path
_propfunc_src_paths(f::PropertyFunctions.PropertyFunction{src_paths}) where src_paths = map(_ppath_path, (src_paths.parameters...,))
_propfunc_src_columnnames(f::PropertyFunctions.PropertyFunction) = map(first, _propfunc_src_paths(f))
_propfunc_trg_columnnames(f::PropSelFunction{src_paths, trg_cols}) where {src_paths, trg_cols} = trg_cols

# The object a property path reads from, and the properties left for the function itself: a
# path may continue into a stored type -- `$waveform.signal` -- past the groups of the file.
function _lh5_path(h, group::AbstractString, path::Tuple)
    p = "$group/$(first(path))"
    for (i, c) in enumerate(Base.tail(path))
        haskey(h, "$p/$c") || return h[p], path[(i + 1):end]
        p = "$p/$c"
    end
    h[p], ()
end

# The values of `paths`, nested the way the paths name them, so that a function that reads
# `$geds.aoe_sg_classifier` finds it there.
function _nest(paths, vals)
    heads = unique(map(first, paths))
    NamedTuple{Tuple(heads)}(map(heads) do head
        i = findall(p -> first(p) == head, collect(paths))
        j = findfirst(k -> length(paths[k]) == 1, i)
        isnothing(j) ? Table(_nest(map(k -> Base.tail(paths[k]), i), map(k -> vals[k], i))) : vals[i[j]]
    end)
end

# Every column of a group keeps the rows `sel` selects; a single column is unwrapped.
# TODO: pass `sel` to the read once `LH5Array` supports `AbstractVector{Int}` indices.
_load_all_keys(nt::NamedTuple, sel=(:)) = length(nt) == 1 ? _load_all_keys(nt[first(keys(nt))], sel) :
    NamedTuple{keys(nt)}(map(k -> _load_all_keys(nt[k], sel), keys(nt)))
_load_all_keys(x::Union{AbstractArray, Table}, sel=(:)) = sel === (:) ? x[:] : x[:][sel]
_load_all_keys(x, sel=(:)) = x

_nrows(nt::NamedTuple) = _nrows(first(nt))
_nrows(x) = length(x)

# Read the table at `group`, or every table below it. Each pair of `filter_pairs` is read
# from its own tier and selects by position, every tier holding one row per trigger.
function _read_lh5_det(h, data::LegendData, tier::DataTier, filekey::FileKey, det::ChannelSel, f::Base.Callable, filter_pairs::Tuple, ignore_missing::Bool,
    group::AbstractString = isnothing(det) ? "$tier" : "$tier/$det")

    if !haskey(h, group)
        ignore_missing || throw(ArgumentError("$group not found in $(basename(string(h.data_store)))"))
        @debug "$group not found in $(basename(string(h.data_store)))"
        return nothing
    end
    paths = f isa PropertyFunctions.PropertyFunction ? _propfunc_src_paths(f) : ()

    # An LH5 `table` is one row set; a `struct` or an unlabelled group holds further groups
    # with their own rows, read one at a time. Keys come from the store: opening reads columns.
    hgroup = h.data_store[group]
    datatype = LegendHDF5IO.getattribute(hgroup, :datatype, "")
    if isempty(datatype) || startswith(datatype, "struct")
        ks = keys(hgroup)
        isempty(ks) && throw(ArgumentError("No data found under /$group in $(basename(string(h.data_store)))"))
        # A function may name groups of this level -- the peaks of `jlpeaks`, say -- rather
        # than columns of the tables below it.
        sub = String.(map(first, paths))
        named = any(in(ks), sub)
        named && !all(in(ks), sub) && throw(ArgumentError(
            "$(join(setdiff(sub, ks), ", ")) not found under /$group in $(basename(string(h.data_store)))"))
        # A selection of whole groups picks them and is used up; a function that reads into
        # them navigates them itself and so is applied to this level, below.
        if named && f isa PropSelFunction && all(p -> length(p) == 1, paths)
            length(sub) == 1 && return _read_lh5_det(h, data, tier, filekey, det, identity,
                filter_pairs, ignore_missing, "$group/$(only(sub))")
            ks, f = sub, identity
        end
        if !(named && f isa PropertyFunctions.PropertyFunction)
            read = collect(map(ks) do k
                # The channels are the level named by `det`, whichever depth it sits at. Not
                # every one of them is a `DetectorId`: a raw tier also holds the electronics.
                child = !isnothing(det) ? det :
                    LegendDataManagement._can_convert_to(DetectorId, k) ? DetectorId(k) : k
                _read_lh5_det(h, data, tier, filekey, child, f, filter_pairs, ignore_missing, "$group/$k")
            end)
            # A child that `ignore_missing` skipped leaves no entry behind.
            keep = findall(!isnothing, read)
            return NamedTuple{Tuple(Symbol.(collect(ks)[keep]))}(Tuple(read[keep]))
        end
    end

    # A column the function names may be missing from a table -- a PMT channel has no
    # waveform -- which is an error unless the caller asked for those to be skipped.
    for path in paths
        haskey(h, "$group/$(first(path))") && continue
        ignore_missing || throw(ArgumentError(
            "$group/$(first(path)) not found in $(basename(string(h.data_store)))"))
        @debug "$group/$(first(path)) not found in $(basename(string(h.data_store)))"
        return nothing
    end

    # Opening a group turns every one of its columns into an `LH5Array`, which dominates the
    # read: a function that names its source paths opens only the leaves they end at.
    lazy = f isa PropertyFunctions.PropertyFunction ?
        map(path -> _lh5_path(h, group, path), paths) : ((h[group], ()),)

    sel = (:)
    for p in filter_pairs
        filter_tier, filter_pf = DataTier(first(p)), last(p)
        # The predicate is read like any other function, giving one row mask per filter tier.
        m = if filter_tier == tier
            _read_lh5_det(h, data, tier, filekey, det, filter_pf, (), ignore_missing, group)
        else
            _lh5_data_open(data, filter_tier, filekey, det) do fh
                # An event tier holds one table for the whole file, not one per detector.
                fgroup = !isnothing(det) && haskey(fh, "$filter_tier/$det") ?
                    "$filter_tier/$det" : "$filter_tier"
                _read_lh5_det(fh, data, filter_tier, filekey, det, filter_pf, (), ignore_missing, fgroup)
            end
        end
        isnothing(m) && return nothing
        length(m) == _nrows(first(first(lazy))) || throw(DimensionMismatch(
            "Filter tier :$filter_tier has $(length(m)) rows but :$tier has $(_nrows(first(first(lazy)))) rows for $det in $filekey"))
        sel = sel === (:) ? coalesce.(m, false) : sel .& coalesce.(m, false)
    end

    vals = map(lazy) do (obj, rest)
        foldl(getproperty, rest; init = _load_all_keys(obj, sel))
    end

    lh5_data = if f isa PropSelFunction
        Table(NamedTuple{_propfunc_trg_columnnames(f)}(vals))
    elseif f isa PropertyFunctions.PropertyFunction
        f.(Table(_nest(_propfunc_src_paths(f), vals)))
    else
        f == identity ? only(vals) : f.(only(vals))
    end
    # A NamedTuple of per-detector or per-system tables stays keyed by its groups.
    lh5_data isa NamedTuple || !TypedTables.Tables.istable(lh5_data) ? lh5_data : Table(lh5_data)
end


function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, FileKey, DetectorSel};
    filterby::Union{Nothing, PropertyFunctions.PropertyFunction, Pair{<:DataTierLike, <:PropertyFunctions.PropertyFunction},
        Tuple{Vararg{Pair{<:DataTierLike, <:PropertyFunctions.PropertyFunction}}}}=nothing,
    ignore_missing::Bool=false, kwargs...)

    tier, filekey, det = rsel

    filterby === () && throw(ArgumentError("`filterby` must name at least one `DataTier`"))
    # A bare PropertyFunction is a predicate on the tier being read.
    filter_pairs = isnothing(filterby) ? () :
        filterby isa PropertyFunctions.PropertyFunction ? (tier => filterby,) :
        filterby isa Pair ? (filterby,) : filterby

    _lh5_data_open(data, tier, filekey, det) do h
        _read_lh5_det(h, data, tier, filekey, det, f, filter_pairs, ignore_missing)
    end
end


LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, FileKey}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], nothing); kwargs...)

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, AbstractVector{FileKey}, DetectorSel}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
    first_fk = first(rsel[2])
    p = Progress(length(rsel[2]), desc="Reading from $(first_fk.setup)-$(first_fk.period)-$(first_fk.run)-$(first_fk.category)", showspeed=true)
    lflatten(if parallel
                # TODO: Check if wpool is connected via :master_worker if myid() != 1
                @debug "Parallel read with $(length(workers())) workers from $(length(rsel[2])) filekeys"
                progress_pmap(wpool, rsel[2]; progress=p) do fk
                    LegendDataManagement.read_ldata(f, data, (rsel[1], fk, rsel[3]); kwargs...)
                end
            else
                @debug "Sequential read from $(length(rsel[2])) filekeys"
                progress_map(rsel[2]; progress=p) do fk
                    LegendDataManagement.read_ldata(f, data, (rsel[1], fk, rsel[3]); kwargs...)
                end
            end)
end
LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, AbstractVector{FileKey}}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], nothing); kwargs...)

# Several detectors are read one at a time and keyed by detector, the rest of the selection
# being whatever it is for a single one. A detector `ignore_missing` skips leaves no entry.
for n_sel in 2:4
    @eval LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData,
        rsel::Tuple{$(fill(:RSelLike, n_sel)...), AbstractVector{<:DetectorIdLike}}; kwargs...) = begin
        dets = last(rsel)
        read = map(d -> LegendDataManagement.read_ldata(f, data, (Base.front(rsel)..., d); kwargs...), dets)
        keep = findall(!isnothing, read)
        NamedTuple{Tuple(Symbol.(dets[keep]))}(Tuple(read[keep]))
    end
end

### Argument distinction for different DataSelector Types
function _convert_rsel2dsel(rsel::Tuple)
    selector_types = [PossibleDataSelectors[LegendDataManagement._can_convert_to.(PossibleDataSelectors, Ref(s))] for s in rsel]
    # The first element of a selection is the tier and the second the category, whatever else
    # their names could stand for.
    if !isempty(selector_types) && DataTier in selector_types[1]
        selector_types[1] = [DataTier]
    end
    if length(selector_types) >= 2 && DataCategory in selector_types[2]
        selector_types[2] = [DataCategory]
    end
    if !all(length.(selector_types) .<= 1)
        throw(ArgumentError("Ambiguous selector types: $selector_types for $rsel"))
    end
    # A `FileKey`, a run table or a vector of them is no selector and stays as it is; an
    # empty detector reads every channel.
    Tuple([r isa AbstractString && isempty(r) ? nothing :
        isempty(st) ? r : only(st)(r) for (r, st) in zip(rsel, selector_types)])
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{Vararg{RSelLike}}; kwargs...)
    dsel = _convert_rsel2dsel(rsel)
    # Converting an already converted selector reaches this method again: no method takes
    # this combination, so recursing on it would not terminate.
    typeof(dsel) == typeof(rsel) && throw(ArgumentError("read_ldata does not support the selector combination $(typeof.(dsel))"))
    LegendDataManagement.read_ldata(f, data, dsel; kwargs...)
end

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], nothing); kwargs...)

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DataRun}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], rsel[4], nothing); kwargs...)


function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPartition, DetectorId}; kwargs...)
    pinfo = partitioninfo(data, rsel[4], rsel[3])
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], pinfo, rsel[4]); kwargs...)
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DetectorSel}; kwargs...)
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], runinfo(data, rsel[3]), rsel[4]); kwargs...)
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DataRun, DetectorSel}; kwargs...)
    fks = search_disk(FileKey, data.tier[rsel[1], rsel[2], rsel[3], rsel[4]])
    det = rsel[5]
    if isempty(fks) && !isnothing(det) && isfile(data.tier[rsel[1:4]..., det])
        LegendDataManagement.read_ldata(f, data, (rsel[1], start_filekey(data, (rsel[3], rsel[4], rsel[2])), det); kwargs...)
    elseif !isempty(fks)
        LegendDataManagement.read_ldata(f, data, (rsel[1], fks, det); kwargs...)
    else
        throw(ArgumentError("No filekeys found for $(rsel[2]) $(rsel[3]) $(rsel[4])"))
    end
end


### DataPartition
const _partinfo_required_cols = NamedTuple{(:period, :run), Tuple{DataPeriod, DataRun}}

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, Table{_partinfo_required_cols}, DetectorSel}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
    p = Progress(length(rsel[3]), desc="Reading from $(length(rsel[3])) runs", showspeed=true)
    lflatten(if parallel
                # TODO: Check if wpool is connected via :master_worker if myid() != 1
                @debug "Parallel read with $(length(workers())) workers from $(length(rsel[3])) runs"
                progress_pmap(wpool, rsel[3]; progress=p) do r
                    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], r.period, r.run, rsel[4]); parallel, wpool, kwargs...)
                end
            else
                @debug "Sequential read from $(length(rsel[3])) runs"
                progress_map(rsel[3]; progress=p) do r
                    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], r.period, r.run, rsel[4]); parallel=false, kwargs...)
                end
            end)
end

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, Table{_partinfo_required_cols}}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], nothing); kwargs...)

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, Table, DetectorSel}; kwargs...)
    @assert (hasproperty(rsel[3], :period) && hasproperty(rsel[3], :run)) "Runtable doesn't provide periods and runs"
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], Table(period = rsel[3].period, run = rsel[3].run), rsel[4]); kwargs...)
end

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, Table}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], nothing); kwargs...)


end # module