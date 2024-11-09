# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

import Test

Test.@testset "Package LegendDataManagement" begin
    # include("test_aqua.jl")

    include("test_filekey.jl")
    include("test_data_config.jl")
    include("test_props_db.jl")
    include("test_legend_data.jl")
    include("test_workers.jl")
    include("test_map_datafiles.jl")
    include("test_ljl_expressions.jl")
    include("test_lpy_expressions.jl")
    include("test_dataprod_config.jl")
    include("test_lprops.jl")
    include("test_exposure.jl")
    include("test_ext_ssd.jl")
    include("test_ext_plots.jl")
    include("test_ext_legendhdf5io.jl")
    include("test_docs.jl")
    isempty(Test.detect_ambiguities(LegendDataManagement))
end # testset
