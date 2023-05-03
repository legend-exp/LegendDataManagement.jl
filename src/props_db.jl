# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const validity_filename = "validity.jsonl"

const _ValidityTimesFiles = NamedTuple{(:valid_from, :filelist), Tuple{Vector{DateTime}, Vector{Vector{String}}}}
const _ValidityDict = IdDict{Symbol,_ValidityTimesFiles}



"""
    struct LegendDataManagement.ValiditySelection

Representy validiy selection for a `LegendDataManagement.PropsDB`[@ref].
"""
struct ValiditySelection
    timestamp::DateTime
    category::Symbol
end

ValiditySelection(timestamp::AbstractString, category::Symbol) = ValiditySelection(timestamp_from_string(timestamp), category)
ValiditySelection(filekey::FileKey) = ValiditySelection(DateTime(filekey), filekey.category)


function _get_validity_sel_filelist(validity::_ValidityDict, category::Symbol, sel_time::DateTime)
    validity_times, validity_filelists = validity[category]
    idx = searchsortedlast(validity_times, sel_time)
    if idx < firstindex(validity_times)
        throw(ArgumentError("Selected validity date $(sel_time) is before first available validity date $(first(validity_times)) for category :$category"))
    end
    return validity_filelists[idx]
end

function _read_validity_sel_filelist(dir_path::AbstractString, validity::_ValidityDict, sel::ValiditySelection)
    filelist = if haskey(validity, sel.category)
        _get_validity_sel_filelist(validity, sel.category, sel.timestamp)
    elseif haskey(validity, :all)
        _get_validity_sel_filelist(validity, :all, sel.timestamp)
    else
        throw(ErrorException("No validity entries for category $category or category all"))
    end

    abs_filelist = joinpath.(Ref(dir_path), filelist)
    return read(PropDict, abs_filelist, subst_pathvar = true, subst_env = true)
end


"""
    struct LegendDataManagement.PropsDB

Use code should not instantiate `PropsDB` directly, use 
[`LegendDataManagement.AnyProps(base_path::AbstractString)`](@ref) instead.
"""
struct PropsDB{VS<:Union{Nothing,ValiditySelection}}
    _base_path::AbstractString
    _rel_path::Vector{String}
    _validity_sel::VS
    _validity::_ValidityDict
    _prop_names::Vector{Symbol}
    _needs_vsel::Bool
end

function Base.:(==)(a::PropsDB, b::PropsDB)
    _base_path(a) == _base_path(b) && _rel_path(a) == _rel_path(b) && _validity_sel(a) == _validity_sel(b) &&
        _validity(a) == _validity(b) && _prop_names(a) == _prop_names(b) && _needs_vsel(a) == _needs_vsel(b)
end



"""
    LegendDataManagement.AnyProps = Union{LegendDataManagement.PropsDB,PropDicts.PropDict}

Properties stored either in a directory managed via
[`LegendDataManagement.PropsDB`][@ref] or loaded from one or several files
into a `PropDicts.PropDict`.

Constructors:

```julia
LegendDataManagement.AnyProps(base_path::AbstractString)
```
"""
const AnyProps = Union{PropsDB,PropDict}

AnyProps(base_path::AbstractString) = _any_props(base_path, String[], nothing, _ValidityDict())

g_state = nothing
function _any_props(base_path::AbstractString, rel_path::Vector{String}, validity_sel::Union{Nothing,ValiditySelection}, prev_validity::_ValidityDict)
    !isdir(base_path) && throw(ArgumentError("PropsDB base path \"$base_path\" is not a directory"))
    new_validity_path = joinpath(base_path, rel_path..., validity_filename)
    new_validity = _load_validity(new_validity_path, prev_validity)

    files_in_dir = readdir(joinpath(base_path, rel_path...))
    validity_filerefs = vcat(vcat(map(x -> x.filelist, values(new_validity))...)...)
    validity_filerefs_found = !isempty(intersect(files_in_dir, validity_filerefs))

    global g_state = (;base_path, rel_path, validity_sel, new_validity, validity_filerefs_found, files_in_dir)
    if validity_filerefs_found
        if !isnothing(validity_sel)
            _read_validity_sel_filelist(joinpath(base_path, rel_path...), new_validity, validity_sel)
        else
            PropsDB(base_path, rel_path, validity_sel, new_validity, Symbol[], true)
        end
    else
        prop_names = filter(!isequal(:__no_property), _md_propertyname.(files_in_dir))
        PropsDB(base_path, rel_path, validity_sel, new_validity, prop_names, false)
    end
end


function _read_jsonl(filename::AbstractString)
    open(filename) do io
        JSON.parse.(filter(!isempty, collect(eachline(io))))
    end
end

function _load_validity(new_validity_path::AbstractString, prev_validity::_ValidityDict)
    if isfile(new_validity_path)
        entries = PropDict.(_read_jsonl(new_validity_path))
        new_validity = _ValidityDict()
        for props in entries
            valid_from = timestamp2datetime(props.valid_from)
            category = Symbol(props.category)
            filelist = props.apply
            dict_entry = get!(new_validity, category, (valid_from = DateTime[], filelist = Vector{String}[]))
            push!(dict_entry.valid_from, valid_from)
            push!(dict_entry.filelist, filelist)
        end
        for key in keys(new_validity)
            dict_entry = new_validity[key]
            idxs = sortperm(dict_entry.valid_from)
            new_validity[key] = (valid_from = dict_entry.valid_from[idxs], filelist = dict_entry.filelist[idxs])
        end
        return new_validity
    else
        return prev_validity
    end
end


_base_path(pd::PropsDB) = getfield(pd, :_base_path)
_rel_path(pd::PropsDB) = getfield(pd, :_rel_path)
_validity_sel(pd::PropsDB) = getfield(pd, :_validity_sel)
_validity(pd::PropsDB) = getfield(pd, :_validity)
_prop_names(pd::PropsDB) = getfield(pd, :_prop_names)
_needs_vsel(pd::PropsDB) = getfield(pd, :_needs_vsel)

_get_path(pd::PropsDB) = joinpath(_base_path(pd), _rel_path(pd)...)


(pd::PropsDB{Nothing})(selection::ValiditySelection) = _any_props(_base_path(pd), _rel_path(pd), selection, _validity(pd))
(pd::PropsDB{Nothing})(timestamp::DateTime, category::Symbol) = pd(ValiditySelection(timestamp, category))
(pd::PropsDB{Nothing})(filekey::FileKey) = pd(ValiditySelection(filekey))
(pd::PropsDB{Nothing})(timestamp::AbstractString, category::Symbol) = pd(timestamp_from_string(timestamp), category)


function Base.getproperty(pd::PropsDB, s::Symbol)
    # Include internal fields:
    if s == :_base_path
        _base_path(pd)
    elseif s == :_rel_path
        _rel_path(pd)
    elseif s == :_validity_sel
        _validity_sel(pd)
    elseif s == :_validity
        _validity(pd)
    elseif s == :_prop_names
        _prop_names(pd)
    elseif s == :_needs_vsel
        _needs_vsel(pd)
    else
        _get_md_property(pd, s)
    end
end

function Base.propertynames(pd::PropsDB)
    if _needs_vsel(pd)
        full_path = joinpath(_base_path(pd), _rel_path(pd)...)
        throw(ArgumentError("Propertynames not available for PropsDB at \"$full_path\" without validity selection"))
    else
        _prop_names(pd)
    end
end

function Base.propertynames(pd::PropsDB, private::Bool)
    props = propertynames(pd)
    private ? vcat([:_base_path, :_rel_path, :_validity_sel, :_validity], props) : props
end


function _get_md_property(pd::PropsDB, s::Symbol)
    new_relpath = push!(copy(_rel_path(pd)), string(s))
    json_filename = joinpath(_get_path(pd), "$s.json")

    if isdir(joinpath(_base_path(pd), new_relpath...))
        _any_props(_base_path(pd), new_relpath, _validity_sel(pd), _validity(pd))
    elseif isfile(json_filename)
        read(PropDict, json_filename, subst_pathvar = true, subst_env = true)
    else
        throw(ArgumentError("Metadata entry doesn't have a property $s"))
    end
end

function _is_metadata_property_filename(filename)
    endswith(filename, ".json") || isdir(filename)
end

function _md_propertyname(rel_filename::AbstractString)
    @assert !contains(rel_filename, "/") && !contains(rel_filename, "\\")
    if !contains(rel_filename, ".")
        Symbol(rel_filename)
    else
        if endswith(rel_filename, ".json")
            Symbol(rel_filename[begin:end-5])
        else
            :__no_property
        end 
    end
end


function Base.show(io::IO, m::MIME"text/plain", pd::PropsDB)
    print(io, nameof(typeof(pd)), "(")
    show(io, m, _base_path(pd))
    print(")")
    for p in _rel_path(pd)
        print(io, ".", p)
    end
    print(io, " ")
    if _needs_vsel(pd)
        print(io, "(validity selection required)")
    else
        show(io, m, propertynames(pd))
    end
end
