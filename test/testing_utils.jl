# This file is a part of LegendDataManagement.jl, licensed under the 

using LegendTestData

joinpath(legend_test_data_path(), "data", "legend")

testdata_dir = joinpath(legend_test_data_path(), "data", "legend")
ENV["LEGEND_DATA_CONFIG"] = joinpath(testdata_dir, "config.json")

normalize_path(path::AbstractString) = replace(path, "\\" => "/")
