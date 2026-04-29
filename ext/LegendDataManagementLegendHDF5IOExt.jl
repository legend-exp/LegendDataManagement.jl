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
    (@isdefined DetectorId) && extend_datatype_dict(DetectorId, "detectorid")
    (@isdefined DataPartition) && extend_datatype_dict(DataPartition, "datapartition")
end

# Tier classes drive dispatch: per-det tiers live in per-detector files; evt
# tiers in shared files. raw and jldsp aren't listed — they're shared files
# but reach detectors via inner paths (handled by _resolve_perdet_path).
const _evt_tiers = DataTier.([:jlevt, :jlskm])
const _perdet_tiers = DataTier.([:jlpeaks, :jlhit, :jlpls])

# Open the LH5 file for (tier, filekey). Per-det tiers also need a det.
function _lh5_data_open(f::Function, data::LegendData, tier::DataTierLike, filekey::FileKey, det::Union{DetectorIdLike, Nothing}=nothing, mode::AbstractString="r")
    t = DataTier(tier)
    if t in _perdet_tiers
        isnothing(det) && throw(ArgumentError("DetectorId required for per-detector tier $t"))
        path = data.tier[t, filekey, DetectorId(det)]
    else
        path = data.tier[t, filekey]
    end
    LegendHDF5IO.lh5open(f, path, mode)
end

_skipnothingmissing(xv::AbstractVector) = [x for x in skipmissing(xv) if !isnothing(x)]
lflatten(x) = fast_flatten(collect(_skipnothingmissing(x)))
lflatten(nt::AbstractVector{<:NamedTuple}) = flatten_by_key(collect(_skipnothingmissing(nt)))

# Source-column names for any PropertyFunction (incl. filterby); target-column
# names only for PropSelFunction (the only kind that renames on read).
_propfunc_src_columnnames(::PropertyFunctions.PropertyFunction{names}) where names = names
_propfunc_src_columnnames(::Any) = ()
_propfunc_trg_columnnames(::PropSelFunction{src, trg}) where {src, trg} = trg

_load_all_keys(nt::NamedTuple, n_evts::Int=-1) = if length(nt) == 1 _load_all_keys(nt[first(keys(nt))], n_evts) else NamedTuple{keys(nt)}(map(x -> _load_all_keys(nt[x], n_evts), keys(nt))) end
_load_all_keys(arr::AbstractArray, n_evts::Int=-1) = arr[:][if (n_evts < 1 || n_evts > length(arr)) 1:length(arr) else rand(1:length(arr), n_evts) end]
_load_all_keys(t::Table, n_evts::Int=-1) = t[:][if (n_evts < 1 || n_evts > length(t)) 1:length(t) else rand(1:length(t), n_evts) end]
_load_all_keys(x, n_evts::Int=-1) = x

# Apply PropSel / filterby / identity to a loaded HDF5 node. Filter is
# missing-tolerant: rows where filterby returns missing are dropped.
function _apply_read(h_node, f::Base.Callable, filterby::Base.Callable, n_evts::Int)
    if f isa PropSelFunction && filterby == Returns(true)
        # PropSel-only fast path: load just the requested leaf columns.
        src_cols = _propfunc_src_columnnames(f)
        trg_cols = _propfunc_trg_columnnames(f)
        Table(if length(src_cols) == 1
            NamedTuple{trg_cols}([_load_all_keys(getproperty(only(src_cols))(h_node), n_evts)])
        else
            NamedTuple{trg_cols}(Tuple(values(columns(_load_all_keys(getproperties(src_cols)(h_node), n_evts)))))
        end)
    else
        lh5_data = _load_all_keys(h_node, n_evts)
        if filterby != Returns(true)
            lh5_data = lh5_data[coalesce.(filterby.(lh5_data), false)]
        end
        f != identity && (lh5_data = f.(lh5_data))
        TypedTables.Tables.istable(lh5_data) ? Table(lh5_data) : lh5_data
    end
end

# Resolve the inner HDF5 path for (tier, det). Primary: "tier/det" (current
# layout). Fallback: "det/tier" (legacy). "tier" alone covers struct-view files
# like raw/jldsp where the per-det entry is itself the tier root.
function _resolve_perdet_path(h, tier::DataTier, det::DetectorId)
    haskey(h, "$tier/$det") && return "$tier/$det"
    haskey(h, "$det/$tier") && return "$det/$tier"
    haskey(h, "$tier")      && return "$tier"
    nothing
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, FileKey, DetectorIdLike}; filterby::Base.Callable=Returns(true), n_evts::Int=-1, ignore_missing::Bool=false, subgroup::Union{Symbol,Nothing}=nothing, kwargs...)
    tier, filekey = DataTier(rsel[1]), rsel[2]
    has_det = !isempty(string(rsel[3]))
    det_arg = has_det ? DetectorId(rsel[3]) : nothing

    data_tier = _lh5_data_open(data, tier, filekey, det_arg) do h
        if tier in _evt_tiers
            # Evt-tier no-det reads return the whole tier table; per-det reads
            # are post-filtered below to events where the detector triggered.
            _apply_read(h["$tier"], f, filterby, n_evts)
        else
            has_det || throw(ArgumentError("DetectorId required for tier $tier"))
            path = _resolve_perdet_path(h, tier, det_arg)
            if path === nothing
                ignore_missing && (@warn "Detector $det_arg not found in $(basename(string(h.data_store)))"; return nothing)
                throw(ArgumentError("Detector $det_arg not found in $(basename(string(h.data_store)))"))
            end
            # Optional descent into a sub-group (e.g. jlhit/<det>/dataQC).
            full = subgroup === nothing ? path : "$path/$subgroup"
            haskey(h, full) || throw(ArgumentError("Subgroup $subgroup not found at $path in $(basename(string(h.data_store)))"))
            _apply_read(h[full], f, filterby, n_evts)
        end
    end
    if tier in _evt_tiers && has_det
        data_tier[any.(map.(isequal(det_arg), data_tier.geds.trig_e_det))]
    else
        data_tier
    end
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, FileKey}; kwargs...)
    tier = DataTier(rsel[1])
    tier in _evt_tiers && return LegendDataManagement.read_ldata(f, data, (tier, rsel[2], ""); kwargs...)
    # Per-det tiers and shared raw/jldsp: list detectors. Primary "tier/<det>"
    # listing falls back to the legacy "<det>/tier" layout (top-level keys).
    dets = _lh5_data_open(data, tier, rsel[2]) do h
        if haskey(h, "$tier")
            collect(keys(h["$tier"]))
        else
            [k for k in keys(h) if haskey(h, "$k/$tier")]
        end
    end
    isempty(dets) && throw(ArgumentError("No detectors found under /$tier in $(basename(data.tier[tier, rsel[2]]))"))
    NamedTuple{Tuple(Symbol.(dets))}([LegendDataManagement.read_ldata(f, data, (tier, rsel[2], d); kwargs...) for d in dets])
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, AbstractVector{FileKey}, DetectorIdLike}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
    first_fk = first(rsel[2])
    p = Progress(length(rsel[2]), desc="Reading from $(first_fk.setup)-$(first_fk.period)-$(first_fk.run)-$(first_fk.category)", showspeed=true)
    lflatten(if parallel
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
    # Disambiguate: a tier symbol like :raw also parses as a generic Symbol.
    if length(selector_types[1]) > 1 && DataTier in selector_types[1]
        selector_types[1] = [DataTier]
    end
    if length(selector_types[2]) > 1 && DataCategory in selector_types[2]
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
    LegendDataManagement.read_ldata(f, data, _convert_rsel2dsel(rsel); kwargs...)
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
    tier = DataTier(rsel[1])
    # Per-det tiers: one file per (run, det) → resolve via start_filekey rather
    # than search_disk (whose tier-dir scan would miss the per-det layout).
    tier in _perdet_tiers && return LegendDataManagement.read_ldata(f, data, (tier, start_filekey(data, (rsel[3], rsel[4], rsel[2])), rsel[5]); kwargs...)
    fks = search_disk(FileKey, data.tier[tier, rsel[2], rsel[3], rsel[4]])
    isempty(fks) && throw(ArgumentError("No filekeys found for $(rsel[2]) $(rsel[3]) $(rsel[4])"))
    LegendDataManagement.read_ldata(f, data, (tier, fks, rsel[5]); kwargs...)
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