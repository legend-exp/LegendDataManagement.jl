module LegendDataManagementLegendHDF5IOExt

using LegendDataManagement
LegendDataManagement._lh5_ext_loaded(::Val{true}) = true
using LegendDataManagement.LDMUtils: detector2channel, channel2detector
using LegendDataManagement: RunCategorySelLike
using LegendHDF5IO
using LegendDataTypes: fast_flatten, flatten_by_key
using StructArrays
using TypedTables, PropertyFunctions
using Distributed, ProgressMeter

const ChannelOrDetectorIdLike = Union{ChannelIdLike, DetectorIdLike}
const AbstractDataSelectorLike = Union{AbstractString, Symbol, DataTierLike, DataCategoryLike, DataPeriodLike, DataRunLike, DataPartitionLike, ChannelOrDetectorIdLike}
const PossibleDataSelectors = [DataTier, DataCategory, DataPeriod, DataRun, DataPartition, ChannelId, DetectorId]

function _is_valid_id_or_tier(data::LegendData, rsel::Union{AnyValiditySelection, RunCategorySelLike}, id::ChannelOrDetectorIdLike)
    if LegendDataManagement._can_convert_to(ChannelId, id) ||  LegendDataManagement._can_convert_to(DetectorId, id) ||  LegendDataManagement._can_convert_to(DataTier, id)
        true  
    else  
        @warn "Skipped $id since it is neither a valid `ChannelId`, `DetectorId` nor a `DataTier`"  
        false  
    end  
end

function _get_channelid(data::LegendData, rsel::Union{AnyValiditySelection, RunCategorySelLike}, det::ChannelOrDetectorIdLike)
    if LegendDataManagement._can_convert_to(ChannelId, det)
        ChannelId(det)
    elseif LegendDataManagement._can_convert_to(DetectorId, det)
        detector2channel(data, rsel, det)
    else
        throw(ArgumentError("$det is neither a ChannelId nor a DetectorId"))
    end
end

function _get_detectorid(data::LegendData, rsel::Union{AnyValiditySelection, RunCategorySelLike}, det::ChannelOrDetectorIdLike)
    if LegendDataManagement._can_convert_to(DetectorId, det)
        DetectorId(det)
    elseif LegendDataManagement._can_convert_to(ChannelId, det)
        channel2detector(data, rsel, det)
    else
        throw(ArgumentError("$det is neither a ChannelId nor a DetectorId"))
    end
end


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

# write AbstractArray{<:LegendDataManagement.DataSelector}
function LegendHDF5IO.create_entry(parent::LHDataStore, name::AbstractString, 
    data::T; kwargs...) where {T <:AbstractArray{<:LegendDataManagement.DataSelector}}
    
    LegendHDF5IO.create_entry(parent, name, string.(data); kwargs...)
    LegendHDF5IO.setdatatype!(parent.data_store[name], T)
    nothing
end

LegendHDF5IO.LH5Array(ds::LegendHDF5IO.HDF5.Dataset, ::Type{<:T}
    ) where {T <: LegendDataManagement.DataSelector} = begin
    
    s = read(ds)
    T(s)
end

function LegendHDF5IO.LH5Array(ds::LegendHDF5IO.HDF5.Dataset, 
    ::Type{<:AbstractArray{<:T, N}}) where {T <: LegendDataManagement.DataSelector, N}

    s = read(ds)
    T.(s)
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

function _lh5_data_open(f::Function, data::LegendData, tier::DataTierLike, filekey::FileKey, det::DetectorIdLike, mode::AbstractString="r")
    det_filename = data.tier[DataTier(tier), filekey, det]
    filename = data.tier[DataTier(tier), filekey]
    if isfile(det_filename)
        @debug "Read from $(basename(det_filename))"
        LegendHDF5IO.lh5open(f, det_filename, mode)
    elseif isfile(filename)
        @debug "Read from $(basename(filename))"
        LegendHDF5IO.lh5open(f, filename, mode)
    else
        throw(ArgumentError("Neither $(basename(filename)) nor $(basename(det_filename)) found"))
    end
end

_skipnothingmissing(xv::AbstractVector) = [x for x in skipmissing(xv) if !isnothing(x)]
lflatten(x) = fast_flatten(collect(_skipnothingmissing(x)))
lflatten(nt::AbstractVector{<:NamedTuple}) = flatten_by_key(collect(_skipnothingmissing(nt)))

_propfunc_src_columnnames(f::PropSelFunction{src_cols, trg_cols}) where {src_cols, trg_cols} = src_cols
_propfunc_trg_columnnames(f::PropSelFunction{src_cols, trg_cols}) where {src_cols, trg_cols} = trg_cols

_load_all_keys(nt::NamedTuple, n_evts::Int=-1) = if length(nt) == 1 _load_all_keys(nt[first(keys(nt))], n_evts) else NamedTuple{keys(nt)}(map(x -> _load_all_keys(nt[x], n_evts), keys(nt))) end
_load_all_keys(arr::AbstractArray, n_evts::Int=-1) = arr[:][if (n_evts < 1 || n_evts > length(arr)) 1:length(arr) else rand(1:length(arr), n_evts) end]
_load_all_keys(t::Table, n_evts::Int=-1) = t[:][if (n_evts < 1 || n_evts > length(t)) 1:length(t) else rand(1:length(t), n_evts) end]
_load_all_keys(x, n_evts::Int=-1) = x

const _evt_tiers = DataTier.([:jlevt, :jlskm])

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, FileKey, ChannelOrDetectorIdLike}; filterby::Base.Callable=Returns(true), filtertier::DataTierLike=first(rsel), n_evts::Int=-1, ignore_missing::Bool=false, parallel::Bool=false, wpool::WorkerPool=default_worker_pool())
    tier, filekey = DataTier(rsel[1]), rsel[2]

    det = if !isempty(string((rsel[3])))
            _get_detectorid(data, rsel[2], rsel[3])
        else
            rsel[3]
    end
    det_tier = tier in _evt_tiers ? "/$tier" : "$det/$tier"
    
    data_tier = _lh5_data_open(data, tier, filekey, det) do h
        if !isempty(string(det)) && !(tier in _evt_tiers) && !haskey(h, "$det")
            if ignore_missing
                @warn "Detector $det not found in $(basename(string(h.data_store)))"
                return nothing
            else
                throw(ArgumentError("Detector $det not found in $(basename(string(h.data_store)))"))
            end
        end
        
        # load detector data
        if f isa PropSelFunction && filterby == Returns(true)
            # if no filter given optimize performance for property selection functions by only loading required columns
            Table(if length(_propfunc_src_columnnames(f)) == 1
                NamedTuple{_propfunc_trg_columnnames(f)}([_load_all_keys(getproperty(only(_propfunc_src_columnnames(f)))(h[det_tier]), n_evts)])
            else
                NamedTuple{_propfunc_trg_columnnames(f)}(Tuple(values(columns(_load_all_keys(getproperties(_propfunc_src_columnnames(f))(h[det_tier]), n_evts)))))
            end)
        else
            lh5_data = _load_all_keys(h[det_tier], n_evts)
            if filterby != Returns(true)
                lh5_data = lh5_data |> PropertyFunctions.filterby(filterby)
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
    if tier in _evt_tiers && !isempty(string(det))
        ch = _get_channelid(data, filekey, det)
        data_tier[any.(map.(isequal(Int(ch)), data_tier.geds.trig_e_ch))]
    else
        data_tier
    end
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, FileKey}; kwargs...)
    ids = _lh5_data_open(data, rsel[1], rsel[2], "") do h
        keys(h)
    end
    ids = filter(x -> _is_valid_id_or_tier(data, rsel[2], x), ids)
    @debug "Found keys: $ids"
    if length(ids) == 1
        if string(only(ids)) == string(rsel[1])
            LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], ""); kwargs...)
        elseif LegendDataManagement._can_convert_to(ChannelId, only(ids)) || LegendDataManagement._can_convert_to(DetectorId, only(ids))
            LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], string(only(ids))); kwargs...)
        else
            throw(ArgumentError("No tier channel or detector found in $(basename(string(h.data_store)))"))
        end
    else
        NamedTuple{Tuple(Symbol.(ids))}([LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], ch); kwargs...) for ch in ids])
    end
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, AbstractVector{FileKey}, ChannelOrDetectorIdLike}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
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


function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPartition, ChannelOrDetectorIdLike}; kwargs...)
    first_run = first(LegendDataManagement._get_partitions(data, :default, rsel[2])[rsel[3]])
    ch = _get_channelid(data, (first_run.period, first_run.run, rsel[2]), rsel[4])
    pinfo = partitioninfo(data, ch, rsel[3])
    @assert ch == _get_channelid(data, (first(pinfo).period, first(pinfo).run, rsel[2]), rsel[4]) "Channel mismatch in partitioninfo"
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], pinfo, ch); kwargs...)
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, ChannelOrDetectorIdLike}; kwargs...)
    rinfo = runinfo(data, rsel[3])
    first_run = first(rinfo)
    ch = if !isempty(string(rsel[4]))
        _get_channelid(data, (first_run.period, first_run.run, rsel[2]), rsel[4])
    else
        string(rsel[4])
    end
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rinfo, ch); kwargs...)
end

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTier, DataCategory, DataPeriod, DataRun, ChannelOrDetectorIdLike}; kwargs...)
    fks = search_disk(FileKey, data.tier[rsel[1], rsel[2], rsel[3], rsel[4]])
    ch = rsel[5]
    if isempty(fks) && isfile(data.tier[rsel[1:4]..., ch])
        LegendDataManagement.read_ldata(f, data, (rsel[1], start_filekey(data, (rsel[3], rsel[4], rsel[2])), ch); kwargs...)
    elseif !isempty(fks)
        LegendDataManagement.read_ldata(f, data, (rsel[1], fks, ch); kwargs...)
    else
        throw(ArgumentError("No filekeys found for $(rsel[2]) $(rsel[3]) $(rsel[4])"))
    end
end


### DataPartition
const _partinfo_required_cols = NamedTuple{(:period, :run), Tuple{DataPeriod, DataRun}}

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table{_partinfo_required_cols}, ChannelOrDetectorIdLike}; parallel::Bool=false, wpool::WorkerPool=default_worker_pool(), kwargs...)
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

function LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table, ChannelOrDetectorIdLike}; kwargs...)
    @assert (hasproperty(rsel[3], :period) && hasproperty(rsel[3], :run)) "Runtable doesn't provide periods and runs"
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], Table(period = rsel[3].period, run = rsel[3].run), rsel[4]); kwargs...)
end

LegendDataManagement.read_ldata(f::Base.Callable, data::LegendData, rsel::Tuple{DataTierLike, DataCategoryLike, Table}; kwargs...) =
    LegendDataManagement.read_ldata(f, data, (rsel[1], rsel[2], rsel[3], ""); kwargs...)


end # module