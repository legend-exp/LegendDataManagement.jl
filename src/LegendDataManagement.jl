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
using Unitful

using Printf: @printf

using IntervalSets: AbstractInterval, ClosedInterval, leftendpoint, rightendpoint
using LRUCache: LRU
using ProgressMeter: @showprogress
using PropertyFunctions: PropertyFunction, @pf
using StaticStrings: StaticString
using Tables: columns

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
include("calibration_functions.jl")

end # module
