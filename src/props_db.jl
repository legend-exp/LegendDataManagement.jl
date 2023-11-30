# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const validity_filename = "validity.jsonl"

const _ValidityTimesFiles = NamedTuple{(:valid_from, :filelist), Tuple{Vector{Timestamp}, Vector{Vector{String}}}}
const _ValidityDict = IdDict{DataCategory,_ValidityTimesFiles}



"""
    struct LegendDataManagement.ValiditySelection

Representy validiy selection for a `LegendDataManagement.PropsDB`[@ref].
"""
struct ValiditySelection
    timestamp::Timestamp
    category::DataCategory
end
export ValiditySelection

ValiditySelection(filekey::FileKey) = ValiditySelection(DateTime(filekey), DataCategory(filekey))


"""
    const AnyValiditySelection = Union{ValiditySelection,FileKey}

Anything that can be used in time/category-based data selection.
"""
const AnyValiditySelection = Union{ValiditySelection,FileKey}
export AnyValiditySelection



function _get_validity_sel_filelist(validity::_ValidityDict, category::DataCategory, sel_time::Timestamp)
    validity_times, validity_filelists = validity[category]
    idx = searchsortedlast(validity_times, sel_time)
    if idx < firstindex(validity_times)
        throw(ArgumentError("Selected validity date $(sel_time) is before first available validity date $(first(validity_times)) for category :$category"))
    end
    return validity_filelists[idx]
end

function _read_validity_sel_filelist(dir_path::String, validity::_ValidityDict, sel::ValiditySelection)
    filelist = if haskey(validity, sel.category)
        _get_validity_sel_filelist(validity, sel.category, sel.timestamp)
    elseif haskey(validity, DataCategory(:all))
        _get_validity_sel_filelist(validity, DataCategory(:all), sel.timestamp)
    else
        throw(ErrorException("No validity entries for category $(sel.category) or category all"))
    end

    abs_filelist = joinpath.(Ref(dir_path), filelist)
    return readprops(abs_filelist)
end


"""
    struct LegendDataManagement.PropsDB

A PropsDB instance, e.g. `myprops`, presents an on-disk directory containing
JSON files or sub-directories (that contains JSON files in leaf directories)
as a dictionary of properties.

`PropsDB` supports `Base.keys` and `Base.getindex` as well as
`Base.propertynames` and `Base.getproperty` to access it's contents.
`getindex` and `getproperty` will return either another `PropsDB`
or a `PropDicts.PropDict`, depending on whether the accessed property
is stored as a sub-directory or a JSON file. We recommend to use
`getproperty` where the properties/keys of the PropDict are more or less
standardized and where they may be arbitrary (see examples below).

The contents of `PropsDB` may be time- and category-dependent, determined by
the presence of a "validity.json" file. In this case, use
`myprops(sel::LegendDataManagement.ValiditySelection)` or
`myprops(filekey::FileKey)` to select the desired time and category. The
selection can be made at some point during traversal of properties or at
the leaf `PropsDB` (see the examples below).

Examples:

```julia
l200 = LegendData(:l200)

propertynames(l200.metadata.hardware)
l200.metadata.hardware.detectors.germanium

keys(l200.metadata.hardware.detectors.germanium.diodes)
l200.metadata.hardware.detectors.germanium.diodes[:V99000A]

diodes = l200.metadata.hardware.detectors.germanium.diodes
diodes[keys(diodes)]

sel = ValiditySelection("20221226T194007Z", :cal)
filekey = FileKey("l200-p02-r006-cal-20221226T194007Z")
data.metadata.hardware(sel).configuration.channelmaps
data.metadata.hardware.configuration.channelmaps(filekey)
```

Use code should *not* instantiate `PropsDB` directly, use 
[`LegendDataManagement.AnyProps(path::AbstractString)`](@ref)
instead, which may return a `PropsDB` or a `PropDicts.PropDict`
depending on what on-disk content `path` points to. 
"""
struct PropsDB{VS<:Union{Nothing,ValiditySelection}} <: AbstractDict{Symbol,AbstractDict}
    _base_path::String
    _rel_path::Vector{String}
    _validity_sel::VS
    _prop_names::Vector{Symbol}
    _needs_vsel::Bool
end

function Base.:(==)(a::PropsDB, b::PropsDB)
    _base_path(a) == _base_path(b) && _rel_path(a) == _rel_path(b) && _validity_sel(a) == _validity_sel(b) &&
        _prop_names(a) == _prop_names(b) && _needs_vsel(a) == _needs_vsel(b)
end


"""
    struct LegendDataManagement.NoSuchPropsDBEntry

Indicates that a given property (path) of a
`LegendDataManagementPropsDB`[@ref] does not exist.

Supports
`PropDicts.writeprops(missing_props::NoSuchPropsDBEntry, props::PropDicts.PropDict)`
to create the missing directories and file for the property path.
"""
struct NoSuchPropsDBEntry
    _base_path::String
    _rel_path::Vector{String}
end

_base_path(@nospecialize(pd::NoSuchPropsDBEntry)) = getfield(pd, :_base_path)
_rel_path(@nospecialize(pd::NoSuchPropsDBEntry)) = getfield(pd, :_rel_path)

function _get_md_property(missing_props::NoSuchPropsDBEntry, s::Symbol)
    NoSuchPropsDBEntry(_base_path(missing_props), push!(copy(_rel_path(missing_props)), string(s)))
end

function PropDicts.writeprops(@nospecialize(missing_props::NoSuchPropsDBEntry), @nospecialize(props::PropDict))
    rp = _rel_path(missing_props)
    dir = joinpath(_base_path(missing_props), rp[begin:end-1]...)
    maybe_dirpath = joinpath(dir, rp[end])
    if isdir(maybe_dirpath)
        throw(ErrorException("Cannot write properties to existing directory \"$maybe_dirpath\", target must be a file"))
    end
    file = "$(rp[end]).json"
    mkpath(dir)
    writeprops(joinpath(dir, file), props)
    nothing
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

AnyProps(base_path::AbstractString) = _any_props(String(base_path), String[], nothing)

function _any_props(base_path::String, rel_path::Vector{String}, validity_sel::Union{Nothing,ValiditySelection})
    !isdir(base_path) && throw(ArgumentError("PropsDB base path \"$base_path\" is not a directory"))
    validity_path = joinpath(base_path, rel_path..., validity_filename)
    validity_info = _load_validity(String(validity_path))

    files_in_dir = String.(readdir(joinpath(base_path, rel_path...)))
    maybe_validity_info = something(validity_info, _ValidityDict())
    validity_filerefs = vcat(vcat(map(x -> x.filelist, values(maybe_validity_info))...)...)
    validity_filerefs_found = !isempty(intersect(files_in_dir, validity_filerefs))
    non_validity_files = setdiff(files_in_dir, validity_filerefs)
    prop_names = filter(!isequal(:__no_property), _md_propertyname.(non_validity_files))

    if !isnothing(validity_info)
        if !isnothing(validity_sel)
            _read_validity_sel_filelist(String(joinpath(base_path, rel_path...)), validity_info, validity_sel)
        else
            PropsDB(base_path, rel_path, validity_sel, prop_names, true)
        end
    else
        PropsDB(base_path, rel_path, validity_sel, prop_names, false)
    end
end


function _read_jsonl(filename::String)
    open(filename) do io
        JSON.parse.(filter(!isempty, collect(eachline(io))))
    end
end

function _load_validity(validity_path::String)
    if isfile(validity_path)
        entries = PropDict.(_read_jsonl(validity_path))
        new_validity = _ValidityDict()
        for props in entries
            valid_from = _timestamp2datetime(props.valid_from)
            # Backward compatibility, fallback from "category" to "select":
            category = haskey(props, :category) ? DataCategory(props.category) : DataCategory(props.select)
            filelist = props.apply
            dict_entry = get!(new_validity, category, (valid_from = FileKey[], filelist = Vector{String}[]))
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
        return nothing
    end
end


_base_path(@nospecialize(pd::PropsDB)) = getfield(pd, :_base_path)
_rel_path(@nospecialize(pd::PropsDB)) = getfield(pd, :_rel_path)
_validity_sel(@nospecialize(pd::PropsDB)) = getfield(pd, :_validity_sel)
_prop_names(@nospecialize(pd::PropsDB)) = getfield(pd, :_prop_names)
_needs_vsel(@nospecialize(pd::PropsDB)) = getfield(pd, :_needs_vsel)

"""
    data_path(pd::LegendDataManagement.PropsDB)

Return the path to the data directory that contains `pd`.
"""
data_path(@nospecialize(pd::PropsDB)) = joinpath(_base_path(pd), _rel_path(pd)...)


function _check_propery_access(pd)
    if _needs_vsel(pd) && isempty(_prop_names(pd))
        full_path = joinpath(_base_path(pd), _rel_path(pd)...)
        throw(ArgumentError("Content access not available for PropsDB at \"$full_path\" without validity selection"))
    end
end


(@nospecialize(pd::PropsDB{Nothing}))(selection::ValiditySelection) = _any_props(_base_path(pd), _rel_path(pd), selection)

function(@nospecialize(pd::PropsDB{Nothing}))(timestamp::Union{DateTime,Timestamp,AbstractString}, category::Union{DataCategory,Symbol,AbstractString})
    pd(ValiditySelection(timestamp, category))
end

(@nospecialize(pd::PropsDB{Nothing}))(filekey::FileKey) = pd(ValiditySelection(filekey))


const MaybePropsDB = Union{PropsDB,NoSuchPropsDBEntry}


function Base.getindex(@nospecialize(pd::MaybePropsDB), a, b, cs...)
    getindex(getindex(pd, a), b, cs...)
end

function Base.getindex(@nospecialize(pd::MaybePropsDB), s::Symbol)
    _get_md_property(pd, s)
end

function Base.getindex(@nospecialize(pd::MaybePropsDB), s::DataSelector)
    getindex(pd, Symbol(string(s)))
end

function Base.getindex(@nospecialize(pd::MaybePropsDB), S::AbstractArray{<:Symbol})
    _get_md_property.(Ref(pd), S)
end


function Base.getproperty(@nospecialize(pd::MaybePropsDB), s::Symbol)
    # Include internal fields:
    if s == :_base_path
        _base_path(pd)
    elseif s == :_rel_path
        _rel_path(pd)
    elseif s == :_validity_sel
        _validity_sel(pd)
    elseif s == :_prop_names
        _prop_names(pd)
    elseif s == :_needs_vsel
        _needs_vsel(pd)
    else
        pd[s]
    end
end

function Base.keys(@nospecialize(pd::PropsDB))
    _check_propery_access(pd)
    _prop_names(pd)
end

Base.propertynames(@nospecialize(pd::PropsDB)) = keys(pd)

function Base.propertynames(@nospecialize(pd::PropsDB), private::Bool)
    props = propertynames(pd)
    private ? vcat([:_base_path, :_rel_path, :_validity_sel, :_validity], props) : props
end


function _get_md_property(@nospecialize(pd::PropsDB), s::Symbol)
    new_relpath = push!(copy(_rel_path(pd)), string(s))
    json_filename = joinpath(data_path(pd), "$s.json")
    _check_propery_access(pd)

    if isdir(joinpath(_base_path(pd), new_relpath...))
        _any_props(_base_path(pd), new_relpath, _validity_sel(pd))
    elseif isfile(json_filename)
        readprops(json_filename)
    else
        if !_needs_vsel(pd) && (isnothing(_validity_sel(pd)) || isempty(_validity_sel(pd)))
            NoSuchPropsDBEntry(_base_path(pd), push!(copy(_rel_path(pd)), string(s)))
        else
            throw(ArgumentError("Metadata entry doesn't have a property $s"))
        end
    end
end


function _is_metadata_property_filename(filename)
    endswith(filename, ".json") || isdir(filename)
end

function _md_propertyname(rel_filename::String)
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


function Base.length(@nospecialize(pd::PropsDB))
    _check_propery_access(pd)
    length(_prop_names(pd))
end

function Base.iterate(@nospecialize(pd::PropsDB))
    _check_propery_access(pd)
    nms = _prop_names(pd)
    i = firstindex(nms)
    (pd[nms[i]], i+1)
end

function Base.iterate(@nospecialize(pd::PropsDB), i::Int)
    nms = _prop_names(pd)
    if checkbounds(Bool, nms, i)
        (pd[nms[i]], i+1)
    else
        nothing
    end
end


function Base.show(io::IO, m::MIME"text/plain", @nospecialize(pd::PropsDB))
    print(io, nameof(typeof(pd)), "(")
    show(io, m, _base_path(pd))
    print(")")
    for p in _rel_path(pd)
        print(io, ".", p)
    end
    print(io, " ")
    if _needs_vsel(pd) && isempty(_prop_names(pd))
        print(io, "(validity selection required)")
    else
        if _needs_vsel(pd)
            println(io, "(validity selection available)")
        end
        show(io, m, propertynames(pd))
    end
end
