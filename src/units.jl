# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

"""
    recursive_uparse(pd::PropDict)

Recursively parse a PropDict and convert all `:val` fields to `Unitful.Quantity` objects while reading the units from the `:unit` fields.
It respects possible errors in the `:err` fields and converts them to `Unitful.Quantity` objects as well.
If there are no other fields in the `:val` field, the `:unit` field is removed from the `PropDict`.
# Returns
- `pd::PropDict` with all `:val` fields converted to `Unitful.Quantity` objects.
"""
function recursive_uparse(pd::PropDict)
    for (key, val) in pd
        if val isa PropDict
            if val isa Unitful.Quantity
                continue
            elseif haskey(val, :unit) && haskey(val, :val) && haskey(val, :err)
                pd[key].val = Unitful.Quantity(val.val, Unitful.uparse(val.unit))
                pd[key].err = Unitful.Quantity(val.err, Unitful.uparse(val.unit))
                delete!(pd[key], :unit)
            elseif haskey(val, :unit) && haskey(val, :val)
                if length(keys(val)) == 2
                    pd[key] = Unitful.Quantity(val.val, Unitful.uparse(val.unit))
                else
                    pd[key].val = Unitful.Quantity(val.val, Unitful.uparse(val.unit))
                    delete!(pd[key], :unit)
                end
            else
                pd[key] = recursive_uparse(val)
            end
        end
    end
    pd
end

"""
    measurement(pd::PropDict)

Recursively parse a PropDict and convert all `:val` fields to `Measurements.Measurement` objects while reading the errors from the `:err` fields.
If there are no other fields in the `:val` field, the `:err` field is removed from the `PropDict`.
# Returns
- `pd::PropDict` with all `:val` fields converted to `Measurements.Measurement` objects.
"""
function recursive_measurement(pd::PropDict)
    for (key, val) in pd
        if val isa PropDict
            if val.val isa Measurements.Measurement 
                continue
            elseif haskey(val, :err) && haskey(val, :val)
                if length(keys(val)) == 2
                    pd[key] = measurement(val.val, val.err)
                else
                    pd[key].val = measurement(val.val, val.err)
                    delete!(pd[key], :err)
                end
            else
                pd[key] = recursive_measurement(val)
            end
        end
    end
    pd
end

function props2lprops(pd::PropDict)
    if haskey(pd, :val) && length(keys(pd)) <= 3 && (haskey(pd, :unit) || haskey(pd, :err))
        if haskey(pd, :unit)
            if haskey(pd, :err)
                Unitful.Quantity(measurement(pd.val, pd.err), Unitful.uparse(pd.unit))
            else
                Unitful.Quantity(pd.val, Unitful.uparse(pd.unit))
            end
        elseif haskey(pd, :err)
            measurement(pd.val, pd.err)
        else
            throw(ArgumentError("props2lprops can't handle PropDict $pd"))
        end
    else
        PropDict(Dict([key => props2lprops(val) for (key, val) in pd]))
    end
end

props2lprops(x) = x
props2lprops(A::AbstractArray) = props2lprops.(A)

function lprops2props(pd::PropDict)
    PropDict(Dict([key => lprops2props(val) for (key, val) in pd]))
end

lprops2props(x) = x
lprops2props(A::AbstractArray) = lprops2props.(A)
lprops2props(x::Unitful.Quantity{<:Real}) = PropDict(:val => x.val, :unit => string(unit(x)))
lprops2props(x::Unitful.Quantity{<:Measurements.Measurement{<:Real}}) = PropDict(:val => Measurements.value(ustrip(x)), :err => Measurements.uncertainty(ustrip(x)), :unit => string(unit(x)))
lprops2props(x::Measurements.Measurement) = PropDict(:val => Measurements.value(x), :err => Measurements.uncertainty(x))


"""
    ustrip(pd::PropDict)

Recursively strip a PropDict and convert all `Unitful.Quantity` objects to their `:val` and `:unit` fields.
It respects possible errors in the `:err` fields and converts them to `Unitful.Quantity` objects as well.
# Returns
- `pd::PropDict` with all `Unitful.Quantity` objects converted to their `:val` and `:unit` fields.
"""
function Unitful.ustrip(pd::PropDict)
    for (key, val) in pd
        if val isa PropDict
            if val.val isa Unitful.Quantity
                pd[key].unit = string(unit(val.val))
                if haskey(pd[key], :err)
                    pd[key].err = ustrip(val.err)
                end
                pd[key].val = ustrip(val.val)
            end
            pd[key] = ustrip(val)
        else
            if val isa Unitful.Quantity
                pd[key] = PropDict()
                pd[key].val = ustrip(val)
                pd[key].unit = string(unit(val))
            end
        end
    end
    pd
end


"""
    mstrip(pd::PropDict)

Recursively strip a PropDict and convert all `Measurements.Measurement` objects to their `:val` and `:err` fields.
# Returns
- `pd::PropDict` with all `Measurements.Measurement` objects converted to their `:val` and `:err` fields.
"""
function mstrip(pd::PropDict)
    for (key, val) in pd
        if val isa PropDict
            if val.val isa Measurements.Measurement
                pd[key].err = Measurements.uncertainty(val.val)
                pd[key].val = Measurements.value(val.val)
            else
                pd[key] = mstrip(val)
            end
        else
            if val isa Unitful.Quantity
                if ustrip(val) isa Measurements.Measurement
                    pd[key] = PropDict()
                    pd[key].err = Measurements.uncertainty(val)
                    pd[key].val = Measurements.value(val)
                end
            elseif val isa Measurements.Measurement
                pd[key] = PropDict()
                pd[key].err = Measurements.uncertainty(val)
                pd[key].val = Measurements.value(val)
            end
        end
    end
    pd
end
export mstrip

"""
    value(pd::PropDict)

Recursively strip a PropDict and convert all `Measurements.Measurement` to their `value` fields.
# Returns
- `pd::PropDict` with all `Measurements.Measurement` objects converted to their `value` fields.
"""
function Measurements.value(pd::PropDict)
    for (key, val) in pd
        if val isa PropDict || val isa Measurements.Measurement || ustrip(val) isa Measurements.Measurement
            pd[key] = Measurements.value(val)
        end
    end
    pd
end
export value

"""
    uncertainty(pd::PropDict)

Recursively strip a PropDict and convert all `Measurements.Measurement` to their `uncertainty` fields.
# Returns
- `pd::PropDict` with all `Measurements.Measurement` objects converted to their `uncertainty` fields.
"""
function Measurements.uncertainty(pd::PropDict)
    for (key, val) in pd
        if val isa PropDict || val isa Measurements.Measurement || ustrip(val) isa Measurements.Measurement
            pd[key] = Measurements.uncertainty(val)
        end
    end
    pd
end
export uncertainty


"""
    readlprops(filename::AbstractString)
    readprops(filenames::Vector{<:AbstractString}) 


Read a PropDict from a file and parse it to `Unitful.Quantity` and `Measurements.Measurement` objects.
# Returns
- `pd::PropDict` with all `:val` fields converted to `Unitful.Quantity` objects and all `:val` fields converted to `Measurements.Measurement` objects.
"""
function readlprops end
export readlprops

readlprops(filename::AbstractString) = props2lprops(readprops(filename))
readlprops(filenames::Vector{<:AbstractString}) = props2lprops(readprops(filenames)) 

"""
    writelprops(f::IO, p::PropDict; write_units::Bool=true, write_errors::Bool=true, mutliline::Bool=true, indent::Int=4)
    writelprops(filename::AbstractString, p::PropDict; multiline::Bool=true, indent::Int=4)
    writelprops(db::PropsDB, key::Union{Symbol, DataSelector}, p::PropDict; kwargs...)

Write a PropDict to a file and strip it to `:val` and `:unit` fields and `:val` and `:err` fields.
"""
function writelprops end
export writelprops

writelprops(io::IO, p::PropDict; multiline::Bool = true, indent::Int = 4) = writeprops(io, lprops2props(p); multiline=multiline, indent=indent)
writelprops(filename::AbstractString, p::PropDict; multiline::Bool = true, indent::Int = 4) = writeprops(filename, lprops2props(p); multiline=multiline, indent=indent)

writelprops(db::PropsDB, key::Union{Symbol, DataSelector}, p::PropDict; kwargs...) = writelprops(joinpath(data_path(db), "$(string(key)).json"), p; kwargs...)