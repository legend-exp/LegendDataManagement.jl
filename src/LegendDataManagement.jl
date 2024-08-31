# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

__precompile__(true)

module LegendDataManagement

using Dates
using UUIDs

import Distributed
import LinearAlgebra
import Pkg

using Glob
using JSON
using PropDicts
using PropertyDicts
using StructArrays
using Unitful
using Measurements
using Measurements: ±

using Printf: @printf

using IntervalSets: AbstractInterval, ClosedInterval, leftendpoint, rightendpoint
using LRUCache: LRU
using ProgressMeter: @showprogress
using PropertyFunctions: PropertyFunction, @pf, filterby, props2varsyms
using StaticStrings: StaticString
import Tables
using Tables: columns
using TypedTables
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
include("map_datafiles.jl")
include("ljl_expressions.jl")
include("lpy_expressions.jl")
include("dataprod_config.jl")
include("calibration_functions.jl")
include("evt_functions.jl")
include("lprops.jl")
include("utils/utils.jl")

@static if !isdefined(Base, :get_extension)
    using Requires
end

function __init__()
    @static if !isdefined(Base, :get_extension)
        @require LegendHDF5IO = "c9265ca6-b027-5446-b1a4-febfa8dd10b0" begin
            include("../ext/LegendDataManagementLegendHDF5IOExt.jl")
        end        
    end
end
end # module