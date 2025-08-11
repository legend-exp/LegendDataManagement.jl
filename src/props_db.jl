# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

const validity_filename = "validity.yaml"

const _ValidityTimesFiles = NamedTuple{(:valid_from, :filelist), Tuple{Vector{Timestamp}, Vector{Vector{String}}}}
const _ValidityDict = IdDict{DataCategory,_ValidityTimesFiles}

_merge_validity_info!!(::Nothing, ::Nothing) = nothing
_merge_validity_info!!(a::_ValidityDict, ::Nothing) = a
_merge_validity_info!!(::Nothing, b::_ValidityDict) = b

function _merge_validity_info!!(a::_ValidityDict, b::_ValidityDict)
    for k in keys(b)
        if haskey(a, k)
            tf_a, tf_b = a[k], b[k]
            vf_a, fl_a = tf_a.valid_from, tf_a.filelist
            vf_b, fl_b = tf_b.valid_from, tf_b.filelist
            vf_new, fl_new = similar(vf_a, 0), similar(fl_a, 0)
            ia, ib = firstindex(vf_a), firstindex(vf_b)
            while ia <= lastindex(vf_a) || ib <= lastindex(vf_b)
                if ib > lastindex(vf_b) || ia <= lastindex(vf_a) && vf_a[ia] < vf_b[ib]
                    push!(vf_new, vf_a[ia])
                    push!(fl_new, fl_a[ia])
                    ia += 1
                elseif ia > lastindex(vf_a) || vf_a[ia] > vf_b[ib]
                    push!(vf_new, vf_b[ib])
                    push!(fl_new, fl_b[ib])
                    ib += 1
                else
                    @assert vf_a[ia] == vf_b[ib]
                    push!(vf_new, vf_a[ia])
                    push!(fl_new, vcat(fl_a[ia], fl_b[ib]))
                    ia += 1
                    ib += 1
                end
            end
            a[k] = (valid_from = vf_new, filelist = fl_new)
        else
            a[k] = b[k]
        end
    end
    return a
end


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

function _read_validity_sel_filelist(primary_path::String, override_path::String, validity::_ValidityDict, sel::ValiditySelection)
    filelist = if haskey(validity, sel.category)
        _get_validity_sel_filelist(validity, sel.category, sel.timestamp)
    elseif haskey(validity, DataCategory(:all))
        _get_validity_sel_filelist(validity, DataCategory(:all), sel.timestamp)
    else
        throw(ErrorException("No validity entries for category $(sel.category) or category all"))
    end

    abs_filelist = Vector{String}()
    for rel_filename in filelist
        primary_filename = joinpath(primary_path, rel_filename)
        override_filename = !isempty(override_path) ? joinpath(override_path, rel_filename) : ""
        if ispath(primary_filename)
            push!(abs_filelist, primary_filename)
            if ispath(override_filename)
                push!(abs_filelist, override_filename)
            end
        elseif ispath(override_filename)
            push!(abs_filelist, override_filename)
        else
            throw(ErrorException("File \"$rel_filename\" referenced by $sel not found"))
        end
    end

    return readlprops(abs_filelist)
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
    _override_base::String
    _rel_path::Vector{String}
    _validity_sel::VS
    _prop_names::Vector{Symbol}
    _needs_vsel::Bool
end

function Base.:(==)(a::PropsDB, b::PropsDB)
    _base_path(a) == _base_path(b) && _override_base(a) == _override_base(b) && _rel_path(a) == _rel_path(b) && _validity_sel(a) == _validity_sel(b) &&
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
_override_base(@nospecialize(pd::NoSuchPropsDBEntry)) = ""
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
    file = "$(rp[end]).yaml"
    mkpath(dir)
    writeprops(joinpath(dir, file), props)
    nothing
end

PropDicts.PropDict(@nospecialize(missing_props::NoSuchPropsDBEntry)) = PropDicts.PropDict()


"""
    LegendDataManagement.AnyProps = Union{LegendDataManagement.PropsDB,PropDicts.PropDict}

Properties stored either in a directory managed via
[`LegendDataManagement.PropsDB`][@ref] or loaded from one or several files
into a `PropDicts.PropDict`.

Constructors:

```julia
LegendDataManagement.AnyProps(base_path::AbstractString; override_base::AbstractString = "")
```
"""
const AnyProps = Union{PropsDB,PropDict}

function AnyProps(base_path::AbstractString; override_base::AbstractString = "")
    return _any_props(String(base_path), String(override_base), String[], nothing)
end

function _any_props(base_path::String, override_base::String, rel_path::Vector{String}, validity_sel::Union{Nothing,ValiditySelection})
    !isdir(base_path) && throw(ArgumentError("PropsDB base path \"$base_path\" is not a directory"))
    full_primary_path = String(joinpath(base_path, rel_path...))
    full_override_path = String(joinpath(override_base, rel_path...))
    if !isdir(full_override_path)
        full_override_path = ""
    end

    validity_primary_path = joinpath(full_primary_path, validity_filename)
    validity_primary_info = _load_validity(String(validity_primary_path))
    validity_info = if !isempty(override_base)
        validity_override_path = joinpath(override_base, rel_path..., validity_filename)
        if ispath(validity_override_path)
            validity_override_info = _load_validity(validity_override_path)
            _merge_validity_info!!(validity_primary_info, validity_override_info)
        else
            validity_primary_info
        end
    else
        validity_primary_info
    end

    files_in_dir = Set(String.(readdir(full_primary_path)))
    if !isempty(full_override_path)
        union!(files_in_dir, Set(String.(readdir(full_override_path))))
    end
    maybe_validity_info = something(validity_info, _ValidityDict())
    validity_filerefs = vcat(vcat(map(x -> x.filelist, values(maybe_validity_info))...)...)
    non_validity_files = collect(setdiff(files_in_dir, validity_filerefs))
    prop_names = filter(!isequal(:__no_property), _md_propertyname.(non_validity_files))

    if !isnothing(validity_info)
        if !isnothing(validity_sel)
            _read_validity_sel_filelist(full_primary_path, full_override_path, validity_info, validity_sel)
        else
            PropsDB(base_path, override_base, rel_path, validity_sel, prop_names, true)
        end
    else
        PropsDB(base_path, override_base, rel_path, validity_sel, prop_names, false)
    end
end


function _load_validity(validity_path::AbstractString; mode_default::AbstractString = "append")
    if isfile(validity_path)
        # TODO: Cannot use readlprops here because this can be a Vector of PropDicts and not a PropDict
        #       Can readlprops be updated so that we can drop the explicit YAML dependency here?
        raw = ParallelProcessingTools.read_files(validity_path) do io
            YAML.load_file(io)
        end
        new_validity = _ValidityDict()

        if !isnothing(raw)
            for props in PropDict.(raw)
                valid_from = Timestamp(props.valid_from)
                categories = DataCategory.(let s = get(props, :category, "all"); s isa AbstractVector ? s : [s]; end)
                filelist = let fk = props.apply; fk isa AbstractString ? [fk] : fk end
                for category in categories
                    dict_entry = get!(new_validity, category, (valid_from = FileKey[], filelist = Vector{String}[]))
                    mode = isempty(dict_entry.filelist) ? "reset" : get(props, :mode, mode_default)
                    if mode == "reset"
                        new = filelist
                    elseif mode == "append"
                        new = deepcopy(last(dict_entry.filelist))
                        append!(new, filelist)
                    elseif mode == "remove"
                        new = deepcopy(last(dict_entry.filelist))
                        filter!(f -> !(f in filelist), new)
                    elseif mode == "replace"
                        length(filelist) != 2 && throw(ArgumentError("Invalid number of elements in replace mode: $(length(filelist))"))
                        remove_file, add_file = filelist
                        new = deepcopy(last(dict_entry.filelist))
                        idx = findall(new .== remove_file)
                        isnothing(idx) && throw(ArgumentError("Cannot replace $(remove_file): does not exist"))
                        deleteat!(new, idx)
                        push!(new, add_file)
                    else
                        throw(ArgumentError("Unknown mode for $(timestamp): $(mode)"))
                    end
                    push!(dict_entry.valid_from, valid_from)
                    push!(dict_entry.filelist, new)
                end
            end
        end   
        new_validity
    end
end


_base_path(@nospecialize(pd::PropsDB)) = getfield(pd, :_base_path)
_override_base(@nospecialize(pd::PropsDB)) = getfield(pd, :_override_base)
_rel_path(@nospecialize(pd::PropsDB)) = getfield(pd, :_rel_path)
_validity_sel(@nospecialize(pd::PropsDB)) = getfield(pd, :_validity_sel)
_prop_names(@nospecialize(pd::PropsDB)) = getfield(pd, :_prop_names)
_needs_vsel(@nospecialize(pd::PropsDB)) = getfield(pd, :_needs_vsel)


"""
    data_path(pd::LegendDataManagement.PropsDB)

Return the path to the data directory that contains `pd`.
"""
data_path(@nospecialize(pd::PropsDB)) = joinpath(_base_path(pd), _rel_path(pd)...)
data_path(@nospecialize(pd::NoSuchPropsDBEntry)) = joinpath(_base_path(pd), _rel_path(pd)...)

function _propsdb_fullpaths(@nospecialize(pd::PropsDB), @nospecialize(sub_paths::AbstractString...))
    pribase, ovrbase = _base_path(pd), _override_base(pd)
    rp = _rel_path(pd)
    primary = joinpath(pribase, rp..., sub_paths...)
    override = isempty(ovrbase) ? "" : joinpath(ovrbase, rp..., sub_paths...)
    return (String(primary)::String, String(override)::String)
end

function _check_propery_access(pd, existing_filename::String="")
    if _needs_vsel(pd) && isempty(_prop_names(pd))
        full_path = first(_propsdb_fullpaths(pd))
        if !isempty(existing_filename)
            @warn "Content access not available for PropsDB at \"$full_path\", but \"$existing_filename\" ispath."
        else
            throw(ArgumentError("Content access not available for PropsDB at \"$full_path\" without validity selection"))
        end
    end
end


(@nospecialize(pd::PropsDB{Nothing}))(selection::ValiditySelection) = _any_props(_base_path(pd), _override_base(pd), _rel_path(pd), selection)

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

    yaml_primary_filename, yaml_override_filename = _propsdb_fullpaths(pd, "$s.yaml")

    if isdir(joinpath(_base_path(pd), new_relpath...))
        _any_props(_base_path(pd), _override_base(pd), new_relpath, _validity_sel(pd))
    elseif ispath(yaml_primary_filename)
        _check_propery_access(pd, yaml_primary_filename)
        if ispath(yaml_override_filename)
            readlprops([yaml_primary_filename, yaml_override_filename])
        else
            readlprops(yaml_primary_filename)
        end
    elseif ispath(yaml_override_filename)
        _check_propery_access(pd, yaml_override_filename)
        readlprops(yaml_override_filename)
    else
        if !_needs_vsel(pd) && (isnothing(_validity_sel(pd)) || isempty(_validity_sel(pd)))
            NoSuchPropsDBEntry(_base_path(pd), push!(copy(_rel_path(pd)), string(s)))
        else
            throw(ArgumentError("Metadata entry doesn't have a property $s"))
        end
    end
end


function _is_metadata_property_filename(filename)
    (endswith(filename, ".json") || !isnothing(match(r"^[^.].*\.yaml", filename)) || isdir(filename)) && filename != validity_filename
end

function _md_propertyname(rel_filename::String)
    @assert !contains(rel_filename, "/") && !contains(rel_filename, "\\")
    if !contains(rel_filename, ".")
        Symbol(rel_filename)
    else
        if (endswith(rel_filename, ".json") || !isnothing(match(r"^[^.].*\.yaml", rel_filename))) && rel_filename != validity_filename
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
