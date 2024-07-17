# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

function _props2lprops(pd::PropDict)
    if haskey(pd, :val) && length(keys(pd)) <= 3 && (haskey(pd, :unit) || haskey(pd, :err))
        if haskey(pd, :unit)
            if haskey(pd, :err)
                Unitful.Quantity.(measurement.(pd.val, pd.err), Unitful.uparse(pd.unit))
            else
                Unitful.Quantity.(pd.val, Unitful.uparse(pd.unit))
            end
        elseif haskey(pd, :err)
            measurement.(pd.val, pd.err)
        else
            throw(ArgumentError("_props2lprops can't handle PropDict $pd"))
        end
    elseif haskey(pd, :unit) && length(keys(pd)) == 1
        Unitful.Quantity(NaN, Unitful.uparse(pd.unit))
    else
        PropDict(Dict([key => _props2lprops(val) for (key, val) in pd]))
    end
end

_props2lprops(x) = x
_props2lprops(A::AbstractArray) = _props2lprops.(A)
_props2lprops(d::Dict) = _props2lprops(PropDict(d))
_props2lprops(nt::NamedTuple) = _props2lprops(PropDict(pairs(nt)))

function _lprops2props(pd::PropDict)
    PropDict(Dict([key => _lprops2props(val) for (key, val) in pd]))
end

_lprops2props(x) = x
_lprops2props(A::AbstractArray) = _lprops2props.(A)
_lprops2props(x::Unitful.Quantity{<:Real}) = PropDict(:val => x.val, :unit => string(unit(x)))
_lprops2props(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}}) = PropDict(:val => Measurements.value(ustrip(x)), :err => Measurements.uncertainty(ustrip(x)), :unit => string(unit(x)))
_lprops2props(x::Measurements.Measurement) = PropDict(:val => Measurements.value(x), :err => Measurements.uncertainty(x))
_lprops2props(d::Dict) = _lprops2props(PropDict(d))
_lprops2props(nt::NamedTuple) = _lprops2props(PropDict(pairs(nt)))

"""
    readlprops(filename::AbstractString)
    readprops(filenames::Vector{<:AbstractString}) 


Read a PropDict from a file and parse it to `Unitful.Quantity` and `Measurements.Measurement` objects.
# Returns
- `pd::PropDict` with all `:val` fields converted to `Unitful.Quantity` objects and all `:val` fields converted to `Measurements.Measurement` objects.
"""
function readlprops end
export readlprops

readlprops(filename::AbstractString) = _props2lprops(readprops(filename))
readlprops(filenames::Vector{<:AbstractString}) = _props2lprops(readprops(filenames))

"""
    writelprops(f::IO, p::PropDict; write_units::Bool=true, write_errors::Bool=true, mutliline::Bool=true, indent::Int=4)
    writelprops(filename::AbstractString, p::PropDict; multiline::Bool=true, indent::Int=4)
    writelprops(db::PropsDB, key::Union{Symbol, DataSelector}, p::PropDict; kwargs...)

Write a PropDict to a file and strip it to `:val` and `:unit` fields and `:val` and `:err` fields.
"""
function writelprops end
export writelprops

writelprops(io::IO, p::PropDict; multiline::Bool = true, indent::Int = 4) = writeprops(io, _lprops2props(p); multiline=multiline, indent=indent)
writelprops(filename::AbstractString, p::PropDict; multiline::Bool = true, indent::Int = 4) = writeprops(filename, _lprops2props(p); multiline=multiline, indent=indent)

writelprops(db::MaybePropsDB, key::Union{Symbol, DataSelector}, p::PropDict; kwargs...) = writelprops(joinpath(mkpath(data_path(db)), "$(string(key)).json"), p; kwargs...)


"""
    get_values(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}})
    get_values(x::Unitful.Quantity{<:Real})
    get_values(pd::PropDict)
    get_values(A::AbstractArray)

Get the value of a `Unitful.Quantity` or `Measurements.Measurement` object or a `PropDict` or an array of `Unitful.Quantity` or `Measurements.Measurement` objects.
"""
function get_values end
export get_values

get_values(x) = x
get_values(pd::PropDict) = PropDict(Dict([key => get_values(val) for (key, val) in pd]))
get_values(A::AbstractArray) = get_values.(A)
get_values(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}}) = Measurements.value(x)
get_values(x::Measurements.Measurement{<:Real}) = Measurements.value(x)


"""
    get_uncertainties(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}})
    get_uncertainties(x::Unitful.Quantity{<:Real})
    get_uncertainties(pd::PropDict)
    get_uncertainties(A::AbstractArray)

Get the uncertainty of a `Unitful.Quantity` or `Measurements.Measurement` object or a `PropDict` or an array of `Unitful.Quantity` or `Measurements.Measurement` objects.
"""
function get_uncertainties end
export get_uncertainties

get_uncertainties(x) = x
get_uncertainties(pd::PropDict) = PropDict(Dict([key => get_uncertainties(val) for (key, val) in pd]))
get_uncertainties(A::AbstractArray) = get_uncertainties.(A)
get_uncertainties(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}}) = Measurements.uncertainty(x)
get_uncertainties(x::Measurements.Measurement{<:Real}) = Measurements.uncertainty(x)
