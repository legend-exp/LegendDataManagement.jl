# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

__precompile__(true)

module LegendDataManagement

using Dates

using Glob
using JSON
using PropDicts
using PropertyDicts
using StructArrays


include("filekey.jl")
include("dataset.jl")
include("data_config.jl")
include("props_db.jl")
include("legend_data.jl")

end # module
