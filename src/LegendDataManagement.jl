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
using Measurements

using Printf: @printf

using IntervalSets: AbstractInterval, ClosedInterval, leftendpoint, rightendpoint
using LRUCache: LRU
using ProgressMeter: @showprogress
using PropertyFunctions: PropertyFunction, @pf, filterby
using StaticStrings: StaticString
import Tables
using Tables: columns
import Markdown
using MIMEs: mime_from_extension

include("legend_report.jl")
include("status_types.jl")
include("atomic_fcreate.jl")
include("calfunc.jl")
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
include("lprops.jl")


end # module
