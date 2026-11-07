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

# A copy of the test metadata with `runlists` (YAML text) as its `datasets/runlists.yaml`, and
# the value of `LEGEND_DATA_CONFIG` that selects it.
function config_with_runlists(runlists::AbstractString)
    meta = joinpath(mktempdir(), "metadata")
    cp(joinpath(testdata_dir, "metadata"), meta)
    chmod(meta, 0o755; recursive = true)   # the artifact's files are read-only
    write(joinpath(meta, "datasets", "runlists.yaml"), runlists)
    override = joinpath(dirname(meta), "override.yaml")
    write(override, "setups:\n  l200:\n    paths:\n      metadata: '$meta'\n")
    return "$override:$julia_config"
end
