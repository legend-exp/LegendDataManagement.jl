# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

__precompile__(true)

module LegendDataManagement

using Dates

import Distributed
import Pkg

using Glob
using JSON
using PropDicts
using PropertyDicts
using StructArrays

using Printf: @printf

using IntervalSets: AbstractInterval, ClosedInterval
using LRUCache: LRU
using ProgressMeter: @showprogress
using PropertyFunctions: PropertyFunction, @pf

include("filekey.jl")
include("dataset.jl")
include("data_config.jl")
include("props_db.jl")
include("legend_data.jl")
include("workers.jl")
include("map_datafiles.jl")
include("ljl_expressions.jl")
include("lpy_expressions.jl")
include("dataprod_config.jl")

end # module
