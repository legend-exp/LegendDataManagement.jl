# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).


export PolCalFunc

struct PolCalFunc{N,T<:Number} <:Function
    params::NTuple{N,T}
end


PolCalFunc{T}(params::T...) = PolCalFunc{length(params),T}(params)


function (f::PolCalFunc{N,T}){N,T,U}(x::U)
    R = promote_type(T, U)
    y = zero(R)
    xn = one(U)
    @inbounds for p in f.params
        y = R(fma(p, xn, y))
        xn *= x
    end
    y
end


function Base.convert{N,T}(::Type{PolCalFunc{N,T}}, dict::Dict)
    funcstr = dict[:func]
    if !(funcstr in ("pol1", "[0]+[1]*x", "pol2"))
        error("Unsupported function \"$funcstr\" for $(PolCalFunc{N,T})")
    end

    p = zeros(T, N)
    for (i,v) in dict[:params]
        p[i + 1] = v
    end
    PolCalFunc{N,T}((p...))
end


function Base.convert{N,T}(::Type{Dict{Int, PolCalFunc{N,T}}}, dict::Dict)
    calfuncs = Dict{Int, PolCalFunc{N,T}}()
    for (ch, caldict) in dict
        calfuncs[ch] = convert(PolCalFunc{2,Float64}, caldict)
    end
    calfuncs
end


const CalFuncDict = Dict{Int,PolCalFunc{2,Float64}}
export CalFuncDict
