# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

import Test

Test.@testset "Package LegendDataManagement" begin
    # include("test_aqua.jl")
    include("test_filekey.jl")
    include("test_data_config.jl")
    include("test_props_db.jl")
    include("test_legend_data.jl")
    include("test_ljl_expressions.jl")
    include("test_lpy_expressions.jl")
    VERSION >= v"1.8.0" && include("test_ext_ssd.jl")
    include("test_docs.jl")
    isempty(Test.detect_ambiguities(LegendDataManagement))
end # testset
