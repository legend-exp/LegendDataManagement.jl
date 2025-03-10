# This file is a part of LegendDataManagement.jl, licensed under the 

using LegendTestData
import YAML

testdata_dir = joinpath(legend_test_data_path(), "data", "legend")
julia_config = joinpath(testdata_dir, "julia-config.yaml")
if !isfile(julia_config)
    c = YAML.load_file(joinpath(testdata_dir, "dataflow-config.yaml"))
    if !haskey(c, "setups") c = Dict("setups" => Dict("l200" => c)) end
    YAML.write_file(julia_config, c)
end
ENV["LEGEND_DATA_CONFIG"] = julia_config

normalize_path(path::AbstractString) = replace(path, "\\" => "/")
