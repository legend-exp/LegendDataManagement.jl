# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).

__precompile__(true)

module GERDAMetadata

include.([
    "filekey.jl",
    "dataset.jl",
    "dataconfig.jl",
    "calibcatalog.jl",
    "gerdadata.jl",
    "calfunc.jl",
])

end # module
