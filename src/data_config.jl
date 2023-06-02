# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


const _data_config_envvar_name = "LEGEND_DATA_CONFIG"


"""
    struct SetupConfig

Data configuration for an experimental setup.

Supports

```julia
setup_data_path(setup, path_components)
```

Examples:

```julia
setup_data_path(setup, ["tier", "raw", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5"])
```
"""
struct SetupConfig
    paths::Vector{Pair{Vector{String}, String}}
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
    SetupConfig(sorted_paths)
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
    setup_data_path(setup::SetupConfig, path_components::AbstractVector{<:AbstractString})
    setup_data_path(setup::SetupConfig, path::AbstractString})

Get the full absolute path for the given `path_components` as configured for `setup`.
"""
function setup_data_path end
export setup_data_path

function setup_data_path(setup::SetupConfig, path_components::AbstractVector{<:AbstractString})
    idx = findlast(Base.Fix1(_path_key_match, path_components) ∘ first, setup.paths)
    isnothing(idx) && throw(ArgumentError("No path configured for $path_components in given setup"))
    (k, v) = setup.paths[idx]
    n = length(eachindex(k))
    @assert k == path_components[begin:begin+n-1]
    joinpath(v, path_components[begin+n:end]...)
end

setup_data_path(setup::SetupConfig, path::AbstractString) = setup_data_path(setup, (split(path, "/")))



"""
    abstract type AbstractSetupData

Subtypes wrap SetupConfig for specific experiments.
"""
abstract type AbstractSetupData end


"""
    LegendDataManagement.get_setup_config(data::AbstractSetupData)::SetupConfig

Must be specialized for each subtype of [`AbstractSetupData`](@ref).
"""
function get_setup_config end


"""
    setup_data_path(setup::AbstractSetupData, path_components::AbstractVector{<:AbstractString})
    setup_data_path(setup::AbstractSetupData, path::AbstractString})

Get the full absolute path for the given `path_components` as configured for `setup`.
"""
setup_data_path(data::AbstractSetupData, path_components::AbstractVector{<:AbstractString}) = setup_data_path(get_setup_config(data), path_components)
setup_data_path(data::AbstractSetupData, path::AbstractString) = setup_data_path(get_setup_config(data), path)



"""
    struct LegendDataConfig

Data configuration multiple experimental setups.

Contains a single field `setups::PropertyDict{Symbol,SetupConfig}`.

Can be read from a config file via `LegendDataConfig(config_filename[s])`, or
simply `LegendDataConfig()` if the environment variable
`\$$_data_config_envvar_name` is set. `\$$_data_config_envvar_name` may
be a list of colon-separated config filenames, which are applied/merged in
reverse order (analog to the order of prioritiy in `\$PATH` and similar).

Example:

```julia
config = LegendDataConfig("/path/to/config.json")
setup = config.setups.l200
setup_data_path(setup, ["tier", "raw", "cal", "p02", "r006", "l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5"])
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
    p = read(PropDict, config_filenames, subst_pathvar = true, subst_env = true)
    LegendDataConfig(p)
end

function LegendDataConfig()
    if haskey(ENV, _data_config_envvar_name)
        config_filenames = reverse(String.(split(ENV[_data_config_envvar_name], ':')))
        LegendDataConfig(config_filenames)
    else
        throw(ErrorException("Environment variable $_data_config_envvar_name not set"))
    end
end
