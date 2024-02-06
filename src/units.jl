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
    else
        PropDict(Dict([key => _props2lprops(val) for (key, val) in pd]))
    end
end

_props2lprops(x) = x
_props2lprops(A::AbstractArray) = _props2lprops.(A)

function _lprops2props(pd::PropDict)
    PropDict(Dict([key => _lprops2props(val) for (key, val) in pd]))
end

_lprops2props(x) = x
_lprops2props(A::AbstractArray) = _lprops2props.(A)
_lprops2props(x::Unitful.Quantity{<:Real}) = PropDict(:val => x.val, :unit => string(unit(x)))
_lprops2props(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}}) = PropDict(:val => Measurements.value(ustrip(x)), :err => Measurements.uncertainty(ustrip(x)), :unit => string(unit(x)))
_lprops2props(x::Measurements.Measurement) = PropDict(:val => Measurements.value(x), :err => Measurements.uncertainty(x))


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

writelprops(db::PropsDB, key::Union{Symbol, DataSelector}, p::PropDict; kwargs...) = writelprops(joinpath(data_path(db), "$(string(key)).json"), p; kwargs...)