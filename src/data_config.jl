# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


const _data_config_envvar_name = "LEGEND_DATA_CONFIG"


"""
    struct SetupConfig

Data configuration for one experimental setup: a mapping from path prefixes to
directories on disk.

Can be created from a `PropDict` with the entries `paths` and (optional)
`dataset`, or by [`LegendDataConfig`](@ref) from a configuration file. Use
[`data_path`](@ref) to resolve paths.

## Fields
* `paths::Vector{Pair{Vector{String}, String}}`: Pairs of a path prefix, split
    into its components (e.g. `["tier", "raw"]`), and the directory the prefix
    maps to. Sorted by prefix; the last matching prefix is used.
* `dataset::String`: Name of the dataset that is used for the setup. Defaults
    to `"valid"` if the configuration does not contain a `dataset` entry.

## Examples
```julia
using LegendDataManagement

config = LegendDataConfig("/path/to/config.json")
setup = config.setups.l200
data_path(setup, "tier", "raw", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")
```

See also [`LegendDataConfig`](@ref).
"""
struct SetupConfig
    paths::Vector{Pair{Vector{String}, String}}
    dataset::String
end
export SetupConfig

function _split_config_pathentry(s::AbstractString)
    String.(if contains(s, '/')
        split(s, '/')
    else
        # Legacy config format support:
        split(s, '_')
    end)
end

function SetupConfig(p::PropDict)
    paths = [_split_config_pathentry(string(k)) => String(v) for (k,v) in p.paths]
    sorted_paths = sort(paths)
    dataset = haskey(p, :dataset) ? String(p.dataset) : "valid"
    SetupConfig(sorted_paths, dataset)
end


function _path_key_match(path::AbstractVector{<:AbstractString}, key::AbstractVector{<:AbstractString})
    n_path = length(eachindex(path))
    n_key = length(eachindex(key))
    if n_path < n_key
        false
    else
        short_path = view(path, firstindex(path):firstindex(path)+n_key-1)
        short_path == key
    end
end


"""
    data_path(setup::SetupConfig, path_components::AbstractString...)

Returns the full absolute path for the given `path_components` as configured
for `setup`.

The path components are split at `"/"`. The directory configured for the last
matching path prefix is used; an `ArgumentError` is thrown if no prefix
matches.

## Examples
```julia
using LegendDataManagement

config = LegendDataConfig("/path/to/config.json")
data_path(config.setups.l200, "tier", "raw", "cal", "p02", "r006")
```

See also [`SetupConfig`](@ref).
"""
function data_path end
export data_path

Base.@deprecate setup_data_path(setup::SetupConfig, args...) data_path(setup, args...)
export setup_data_path

"""
    setup_data_path(setup, path_components...)

Deprecated. Use [`data_path`](@ref) instead.
"""
setup_data_path


function data_path(setup::SetupConfig, path_components::AbstractString...)
    pc_vec = vcat([split(s, "/") for s in path_components]...)
    idx = findlast(Base.Fix1(_path_key_match, pc_vec) ∘ first, setup.paths)
    isnothing(idx) && throw(ArgumentError("No path configured for \"$(joinpath(pc_vec...))\" in given setup"))
    (k, v) = setup.paths[idx]
    n = length(eachindex(k))
    @assert k == pc_vec[begin:begin+n-1]
    joinpath(v, pc_vec[begin+n:end]...)
end

Base.@deprecate data_path(setup::SetupConfig, path_components::AbstractVector{<:AbstractString}) data_path(setup, path_components...)



"""
    abstract type AbstractSetupData

Supertype of types that wrap a [`SetupConfig`](@ref) for a specific
experiment, e.g. [`LegendData`](@ref).

Subtypes have to implement `LegendDataManagement.get_setup_config`.
"""
abstract type AbstractSetupData end


"""
    LegendDataManagement.get_setup_config(data::AbstractSetupData)::SetupConfig

Returns the [`SetupConfig`](@ref) of `data`.

Has to be specialized for each subtype of [`AbstractSetupData`](@ref).
"""
function get_setup_config end


"""
    data_path(setup::AbstractSetupData, path_components::AbstractString...)

Returns the full absolute path for the given `path_components` as configured
for `setup`, using the [`SetupConfig`](@ref) of `setup`.

## Examples
```julia
using LegendDataManagement

l200 = LegendData(:l200)
data_path(l200, "tier", "raw", "cal", "p02", "r006")
```
"""
data_path(data::AbstractSetupData, path_components::AbstractString...) = data_path(get_setup_config(data), path_components...)
Base.@deprecate data_path(data::AbstractSetupData, path_components::AbstractVector{<:AbstractString}) data_path(data, path_components...)

Base.@deprecate setup_data_path(setup::AbstractSetupData, args...) data_path(setup, args...)



"""
    struct LegendDataConfig

    LegendDataConfig()
    LegendDataConfig(config_filename::AbstractString)
    LegendDataConfig(config_filenames::AbstractArray{<:AbstractString})
    LegendDataConfig(p::PropDict)

Data configuration for multiple experimental setups.

Contains a single field `setups::PropertyDict{Symbol,SetupConfig}`.

Can be read from a config file via `LegendDataConfig(config_filename[s])`, or
simply `LegendDataConfig()` if the environment variable
`\$$_data_config_envvar_name` is set; an `ErrorException` is thrown if it is
not set. `\$$_data_config_envvar_name` may be a list of colon-separated config
filenames, which are applied/merged in reverse order (analogous to the order
of priority in `\$PATH` and similar).

## Examples
```julia
using LegendDataManagement

config = LegendDataConfig("/path/to/config.json")
setup = config.setups.l200
data_path(setup, "tier", "raw", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")
```

See also [`SetupConfig`](@ref).
"""
struct LegendDataConfig
    setups::PropertyDict{Symbol,SetupConfig}
end
export LegendDataConfig


function LegendDataConfig(p::PropDict)
    setups = Dict{Symbol,SetupConfig}()
    for (k, v) in p.setups
        setups[k] = SetupConfig(v)
    end
    LegendDataConfig(setups)
end

function LegendDataConfig(config_filenames::Union{AbstractString,AbstractArray{<:AbstractString}})
    p = readprops(config_filenames)
    LegendDataConfig(p)
end

function LegendDataConfig()
    if haskey(ENV, _data_config_envvar_name)
        config_filenames = reverse(String.(split(ENV[_data_config_envvar_name], r":(?!\\)")))
        LegendDataConfig(config_filenames)
    else
        throw(ErrorException("Environment variable $_data_config_envvar_name not set"))
    end
end
