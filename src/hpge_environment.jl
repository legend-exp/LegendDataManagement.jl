"""
    struct HPGeEnvironment{T}

Struct to describe the environment of a HPGe detector which are not set in metadata.

## Parametric types
* `T`: Temperature type

## Fields
* `medium::AbstractString`: Name of the medium.
* `Temperature::T`: Temperature of detector/medium with units. 

## Default constructor for vacuum cryostat at 78 K:

* `HPGeEnvironment()`.
"""

struct HPGeEnvironment{T<: Number}
    medium::AbstractString
    temperature::T
    
    HPGeEnvironment(m::AbstractString,t::T) where {T} = new{T}(m,t)
end

HPGeEnvironment() = HPGeEnvironment("vacuum", 78u"K")
