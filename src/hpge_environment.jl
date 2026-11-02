"""
    struct HPGeEnvironment{T<:Unitful.Temperature{<:Number}}

    HPGeEnvironment()
    HPGeEnvironment(medium::AbstractString, temperature)

Description of the environment of an HPGe detector, which is not part of the
metadata.

`HPGeEnvironment()` describes a vacuum cryostat at 78 K.

## Parametric types
* `T`: Temperature type, a temperature with units.

## Fields
* `medium::AbstractString`: Name of the medium, e.g. `"vacuum"`.
* `temperature::T`: Temperature of the detector and the medium, with units.

## Examples
```julia
using LegendDataManagement, Unitful

env = HPGeEnvironment()
env = HPGeEnvironment("vacuum", 90u"K")
```
"""
struct HPGeEnvironment{T<:Unitful.Temperature{<:Number}}
    medium::AbstractString
    temperature::T
    
    HPGeEnvironment(m::AbstractString,t::T) where {T} = new{T}(m,t)
end

HPGeEnvironment() = HPGeEnvironment("vacuum", 78u"K")
