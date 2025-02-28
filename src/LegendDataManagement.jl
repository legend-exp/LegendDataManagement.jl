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
using UnitfulAtomic
using Measurements
using Measurements: ±, value, uncertainty

using Printf: @printf

using IntervalSets: AbstractInterval, ClosedInterval, leftendpoint, rightendpoint
using LRUCache: LRU
using OhMyThreads: @tasks, tmapreduce
using ProgressMeter: @showprogress
using PropertyFunctions: PropertyFunction, @pf, filterby, sortby, props2varsyms, PropSelFunction
using StaticStrings: StaticString
import Tables
using Tables: columns
using TypedTables
import Markdown
using MIMEs: mime_from_extension

export HPGeEnvironment

include("legend_report.jl")
include("status_types.jl")
include("atomic_fcreate.jl")
include("filekey.jl")
include("hpge_environment.jl")
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
include("data_io.jl")
include("active_volume.jl")
include("exposure.jl")
include("utils/utils.jl")

end # module