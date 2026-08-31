module LegendDataManagementLegendHDF5IOExt

using LegendDataManagement
LegendDataManagement._lh5_ext_loaded(::Val{true}) = true
using LegendDataManagement: RunCategorySelLike
using LegendHDF5IO
using LegendDataTypes: fast_flatten, flatten_by_key
using StructArrays
using TypedTables, PropertyFunctions
using Distributed, ProgressMeter


const AbstractDataSelectorLike = Union{AbstractString, Symbol, DataTierLike, DataCategoryLike, DataPeriodLike, DataRunLike, DataPartitionLike, DetectorIdLike}
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
function _lh5_data_open(f::Function, data::LegendData, tier::DataTierLike, filekey::FileKey, det::DetectorIdLike, mode::AbstractString="r")
    t = DataTier(tier)
    det_filename = isempty(string(det)) ? nothing : data.tier[t, filekey, det]
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
lflatten(nt::AbstractVector{<:NamedTuple}) = flatten_by_key(collect(_skipnothingmissing(nt)))

# The source paths are a Tuple type of PPath types (PropertyFunctions 0.3);
# reading a nested path from disk requires its top-level column:
_ppath_path(::Type{PropertyFunctions.PPath{path}}) where path = path
_propfunc_src_columnnames(f::PropertyFunctions.PropertyFunction{src_paths}) where src_paths = map(P -> first(_ppath_path(P)), (src_paths.parameters...,))
_propfunc_trg_columnnames(f::PropSelFunction{src_paths, trg_cols}) where {src_paths, trg_cols} = trg_cols

# `filterby` is a predicate evaluated on the tier being read, a `tier => predicate` pair, or
# a tuple of such pairs (see `_read_ldata_crosstier`).
_filter_pairs(p::Pair) = (DataTier(first(p)) => last(p),)
_filter_pairs(t::Tuple) = isempty(t) ?
    throw(ArgumentError("filterby must name at least one tier")) : map(_filter_pairs_one, t)
_filter_pairs_one(p::Pair) = DataTier(first(p)) => last(p)
_filter_pairs_one(x) = throw(ArgumentError(
    "every element of a filterby tuple must be a `DataTierLike => PropertyFunction` pair, got $(typeof(x))"))

# Row indices of a random subsample without replacement, `nothing` for a full read.
function _sample_idx(n::Int, n_evts::Int)
    (n_evts < 1 || n_evts >= n) && return nothing
    idx = collect(1:n)
    for i in 1:n_evts
        j = rand(i:n)
        idx[i], idx[j] = idx[j], idx[i]
    end
    sort!(resize!(idx, n_evts))
end

_nrows(nt::NamedTuple) = _nrows(first(nt))
_nrows(x::Union{AbstractArray, Table}) = length(x)
_nrows(x) = 0

_sample_rows(x, n_evts::Int) = (idx = _sample_idx(_nrows(x), n_evts); isnothing(idx) ? x : x[idx])

# One index for the whole read, so every column keeps the same rows.
_load_all_keys(x, n_evts::Int=-1) = _take_rows(x, _sample_idx(_nrows(x), n_evts))

_take_rows(nt::NamedTuple, idx) = length(nt) == 1 ? _take_rows(nt[first(keys(nt))], idx) :
    NamedTuple{keys(nt)}(map(k -> _take_rows(nt[k], idx), keys(nt)))
_take_rows(x::Union{AbstractArray, Table}, idx) = isnothing(idx) ? x[:] : x[:][idx]
_take_rows(x, idx) = x

const _evt_tiers = DataTier.([:jlevt, :jlskm])

# Read one detector from an open store, so several detectors of one file can share it.
function _read_lh5_det(h, tier::DataTier, det, f::Base.Callable, filter_pf::Base.Callable, n_evts::Int, ignore_missing::Bool)
    det_tier = tier in _evt_tiers || isempty(string(det)) ? "/$tier" : "$tier/$det"
    if !isempty(string(det)) && !(tier in _evt_tiers) && !haskey(h, det_tier)
        if ignore_missing
            @warn "Detector $det not found in $(basename(string(h.data_store)))"
            return nothing
        else
            throw(ArgumentError("Detector $det not found in $(basename(string(h.data_store)))"))
        end
    end

    # load detector data
    if f isa PropSelFunction && filter_pf == Returns(true)
        # if no filter given optimize performance for property selection functions by only loading required columns
        Table(if length(_propfunc_src_columnnames(f)) == 1
            NamedTuple{_propfunc_trg_columnnames(f)}([_load_all_keys(getproperty(only(_propfunc_src_columnnames(f)))(h[det_tier]), n_evts)])
        else
            NamedTuple{_propfunc_trg_columnnames(f)}(Tuple(values(columns(_load_all_keys(getproperties(_propfunc_src_columnnames(f))(h[det_tier]), n_evts)))))
        end)
    else
        lh5_data = if filter_pf == Returns(true)
            _load_all_keys(h[det_tier], n_evts)
        else
            # Subsample the rows that pass the filter, not the rows on disk.
            _sample_rows(_load_all_keys(h[det_tier]) |> PropertyFunctions.filterby(filter_pf), n_evts)
        end
        if f != identity
            lh5_data = f.(lh5_data)
        end
        if TypedTables.Tables.istable(lh5_data)
            Table(lh5_data)
        else
            lh5_data
        end
    end
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, FileKey, DetectorIdLike}; filterby::Union{Base.Callable, Pair, Tuple}=Returns(true), n_evts::Int=-1, ignore_missing::Bool=false, parallel::Bool=false, wpool::WorkerPool=default_worker_pool())
    tier, filekey = DataTier(rsel[1]), rsel[2]

    filter_pf = filterby
    if !(filterby isa Base.Callable)
        pairs = _filter_pairs(filterby)
        # A single pair naming the tier being read is an ordinary filter on that tier.
        if length(pairs) == 1 && first(first(pairs)) == tier
            filter_pf = last(first(pairs))
        else
            return _read_ldata_crosstier(f, data, tier, filekey, rsel[3], pairs; n_evts, ignore_missing)
        end
    end

    det = !isempty(string(rsel[3])) ? DetectorId(rsel[3]) : rsel[3]

    data_tier = _lh5_data_open(data, tier, filekey, det) do h
        _read_lh5_det(h, tier, det, f, filter_pf, n_evts, ignore_missing)
    end
    # jlevt+det: keep events where the detector triggered (per-event entry in trig_e_det).
    if tier in _evt_tiers && !isempty(string(det))
        data_tier[any.(map.(isequal(det), data_tier.geds.trig_e_det))]
    else
        data_tier
    end
end

# Evaluate each `tier => predicate` pair on its own tier and keep the rows of `tier` that
# every one of them selects. Rows correspond by position: the tiers hold one row per
# trigger of the same detector, so row i of a filter tier describes row i of `tier`. The
# row counts must therefore match.
# TODO: switch to a lazy read once LegendHDF5IO supports scattered index reads
# (`getindex` on `LH5Array` for `AbstractVector{Int}`). The row mask can then be pushed
# into the target read instead of materializing every row and filtering in memory.
function _read_ldata_crosstier(f::Base.Callable, data::LegendData, tier::DataTier, filekey::FileKey, detsel, pairs; n_evts::Int=-1, ignore_missing::Bool=false)
    isempty(string(detsel)) && throw(ArgumentError("Filtering :$tier by $(join(map(p -> ":$(first(p))", pairs), ", ")) requires a DetectorId"))
    det = DetectorId(detsel)

    mask, mask_tier = nothing, nothing
    for (filter_tier, filter_pf) in pairs
        filter_pf isa PropertyFunctions.PropertyFunction || throw(ArgumentError(
            "Filtering :$tier by :$filter_tier requires a `@pf` PropertyFunction that names its source columns, got $(typeof(filter_pf))"))
        filter_data = LegendDataManagement.read_ldata(_propfunc_src_columnnames(filter_pf), data, (filter_tier, filekey, det))
        m = coalesce.(filter_pf.(filter_data), false)
        if isnothing(mask)
            mask, mask_tier = m, filter_tier
        else
            length(m) == length(mask) || throw(DimensionMismatch(
                "Filter tier :$filter_tier has $(length(m)) rows but :$mask_tier has $(length(mask)) rows for $det in $filekey"))
            mask = mask .& m
        end
    end

    lh5_data = LegendDataManagement.read_ldata(f, data, (tier, filekey, det); ignore_missing)
    isnothing(lh5_data) && return nothing
    length(mask) == length(lh5_data) || throw(DimensionMismatch(
        "Filter tier :$mask_tier has $(length(mask)) rows but :$tier has $(length(lh5_data)) rows for $det in $filekey"))

    _sample_rows(lh5_data[mask], n_evts)
end

# Every detector listed here lives in the one file the store was opened on, so all of
# them are read from that single open store.
function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, FileKey}; filterby::Union{Base.Callable, Pair, Tuple}=Returns(true), n_evts::Int=-1, ignore_missing::Bool=false, parallel::Bool=false, wpool::WorkerPool=default_worker_pool())
    tier = DataTier(rsel[1])

    filter_pf = filterby
    if !(filterby isa Base.Callable)
        pairs = _filter_pairs(filterby)
        # A cross-tier filter needs a detector to align the tiers on.
        if length(pairs) == 1 && first(first(pairs)) == tier
            filter_pf = last(first(pairs))
        else
            return _read_ldata_crosstier(f, data, tier, rsel[2], "", pairs; n_evts, ignore_missing)
        end
    end

    _lh5_data_open(data, tier, rsel[2], "") do h
        # Detector ids sit under the tier group (tier/<det>); event-tier files hold only the
        # tier group itself. Listed via the HDF5 group: h["$tier"] would read the whole group.
        valid(x) = LegendDataManagement._can_convert_to(DetectorId, x) ||
                   LegendDataManagement._can_convert_to(DataTier, x)
        ids = haskey(h, "$tier") ? filter(x -> LegendDataManagement._can_convert_to(DetectorId, x), keys(h.data_store["$tier"])) : String[]
        isempty(ids) && (ids = filter(valid, keys(h)))
        @debug "Found keys: $ids"
        isempty(ids) && throw(ArgumentError("No `DetectorId` or `DataTier` key found in $(basename(string(h.data_store)))"))
        read_det(d) = _read_lh5_det(h, tier, d, f, filter_pf, n_evts, ignore_missing)
        if length(ids) == 1
            read_det(string(only(ids)) == string(tier) ? "" : string(only(ids)))
        else
            NamedTuple{Tuple(Symbol.(ids))}([read_det(d) for d in ids])
        end
    end
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, AbstractVector{FileKey}, DetectorIdLike}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
    first_fk = first(rsel[2])
    p = Progress(length(rsel[2]), desc="Reading from $(first_fk.setup)-$(first_fk.period)-$(first_fk.run)-$(first_fk.category)", showspeed=true)
    lflatten(if parallel
                # TODO: Check if wpool is connected via :master_worker if myid() != 1
                @debug "Parallel read with $(length(workers())) workers from $(length(rsel[2])) filekeys"
                progress_pmap(wpool, rsel[2]; progress=p) do fk
                    LegendDataManagement.read_ldata(f, data, ifelse(!isempty(string(rsel[3])), (rsel[1], fk, rsel[3]),  (rsel[1], fk)); kwargs...)
                end
            else
                @debug "Sequential read from $(length(rsel[2])) filekeys"
                progress_map(rsel[2]; progress=p) do fk
                    LegendDataManagement.read_ldata(f, data, ifelse(!isempty(string(rsel[3])), (rsel[1], fk, rsel[3]),  (rsel[1], fk)); kwargs...)
                end
            end)
end
LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, AbstractVector{FileKey}}; kwargs...) = 
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], ""); kwargs...)

### Argument distinction for different DataSelector Types
function _convert_rsel2dsel(rsel::NTuple{<:Any, AbstractDataSelectorLike})
    selector_types = [PossibleDataSelectors[LegendDataManagement._can_convert_to.(PossibleDataSelectors, Ref(s))] for s in rsel]
    if length(selector_types) >= 2 && length(selector_types[2]) > 1 && DataCategory in selector_types[2]
        selector_types[2] = [DataCategory]
    end
    if isempty(last(selector_types))
        selector_types[end] = [String]
    end
    if !all(length.(selector_types) .<= 1)
        throw(ArgumentError("Ambiguous selector types: $selector_types for $rsel"))
    end
    Tuple([only(st)(r) for (r, st) in zip(rsel, selector_types)])
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::NTuple{<:Any, AbstractDataSelectorLike}; kwargs...)
    dsel = _convert_rsel2dsel(rsel)
    # Converting an already converted selector reaches this method again: no method takes
    # this combination, so recursing on it would not terminate.
    typeof(dsel) == typeof(rsel) && throw(ArgumentError("read_ldata does not support the selector combination $(typeof.(dsel))"))
    LegendDataManagement.read_ldata(f, data, dsel; kwargs...)
end

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (DataTier(rsel[1]), DataCategory(rsel[2]), DataPeriod(rsel[3]), ""); kwargs...)

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DataRun}; kwargs...) = 
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], rsel[4], ""); kwargs...)


function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPartition, DetectorIdLike}; kwargs...)
    pinfo = partitioninfo(data, DetectorId(rsel[4]), rsel[3])
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], pinfo, rsel[4]); kwargs...)
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DetectorIdLike}; kwargs...)
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], runinfo(data, rsel[3]), rsel[4]); kwargs...)
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DataRun, DetectorIdLike}; kwargs...)
    fks = search_disk(FileKey, data.tier[rsel[1], rsel[2], rsel[3], rsel[4]])
    det = rsel[5]
    if isempty(fks) && isfile(data.tier[rsel[1:4]..., det])
        LegendDataManagement.read_ldata(f, data, (rsel[1], start_filekey(data, (rsel[3], rsel[4], rsel[2])), det); kwargs...)
    elseif !isempty(fks)
        LegendDataManagement.read_ldata(f, data, (rsel[1], fks, det); kwargs...)
    else
        throw(ArgumentError("No filekeys found for $(rsel[2]) $(rsel[3]) $(rsel[4])"))
    end
end


### DataPartition
const _partinfo_required_cols = NamedTuple{(:period, :run), Tuple{DataPeriod, DataRun}}

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table{_partinfo_required_cols}, DetectorIdLike}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
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

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table{_partinfo_required_cols}}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], ""); kwargs...)

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table, DetectorIdLike}; kwargs...)
    @assert (hasproperty(rsel[3], :period) && hasproperty(rsel[3], :run)) "Runtable doesn't provide periods and runs"
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], Table(period = rsel[3].period, run = rsel[3].run), rsel[4]); kwargs...)
end

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], ""); kwargs...)


end # module