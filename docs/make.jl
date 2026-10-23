# Use
#
#     DOCUMENTER_DEBUG=true GKSwstype=100 julia --color=yes make.jl local [nonstrict] [fixdoctests]
#
# for local builds, run from the `docs/` directory with `--project=.`.

using Documenter
using Literate
using LegendDataManagement

# Load the weak dependencies so that the package extensions are active.
using LegendHDF5IO, SolidStateDetectors, Plots

function fix_literate_output(content)
    content = replace(content, "EditURL = \"@__REPO_ROOT_URL__/\"" => "")
    return content
end

gen_content_dir = joinpath(@__DIR__, "src", "tutorials")
for tut_lit_fn in filter(fn -> endswith(fn, "_lit.jl"), readdir(gen_content_dir))
    lit_src_fn = joinpath(gen_content_dir, tut_lit_fn)
    tut_basename = tut_lit_fn[1:end-7] # remove "_lit.jl"
    Literate.markdown(lit_src_fn, gen_content_dir, name = tut_basename, documenter = true, credit = true, postprocess = fix_literate_output)
    Literate.notebook(lit_src_fn, gen_content_dir, execute = false, name = tut_basename, documenter = true, credit = true)
    Literate.script(lit_src_fn, gen_content_dir, keep_comments = false, name = tut_basename, documenter = true, credit = false)
end

# Extension modules; `get_extension` returns `nothing` if an extension is not loaded.
ext_modules = map((:LegendDataManagementLegendHDF5IOExt, :LegendDataManagementPlotsExt, :LegendDataManagementSolidStateDetectorsExt)) do name
    ext = Base.get_extension(LegendDataManagement, name)
    isnothing(ext) && error("Extension $name did not load")
    ext
end

# Doctest setup
DocMeta.setdocmeta!(
    LegendDataManagement,
    :DocTestSetup,
    :(using LegendDataManagement);
    recursive=true,
)

makedocs(
    sitename = "LegendDataManagement.jl",
    modules = [LegendDataManagement, LegendDataManagement.LDMUtils, ext_modules...],
    format = Documenter.HTML(
        prettyurls = !("local" in ARGS),
        canonical = "https://legend-exp.github.io/LegendDataManagement.jl/stable/",
        size_threshold = nothing,
        size_threshold_warn = nothing,
        example_size_threshold = nothing,
    ),
    pages = [
        "Home" => "index.md",
        "Manual" => Any[
            "Configuration and LegendData" => "man/legenddata.md",
            "Identifiers and FileKeys" => "man/identifiers.md",
            "Data Tiers and Files" => "man/tiers_files.md",
            "Reading Data" => "man/read_data.md",
            "Metadata and Validity" => "man/metadata_validity.md",
            "Channel, Run and Partition Information" => "man/channel_run_info.md",
            "Exposure and Livetime" => "man/exposure.md",
            "Parameters and Property Files" => "man/parameters.md",
            "Property Functions and Expressions" => "man/propfuncs.md",
            "Reports and Utilities" => "man/reports_utils.md",
            "Extensions" => "man/extensions.md",
            "Use in Juleana.jl" => "man/juleana.md",
        ],
        "Tutorials" => [
            "tutorials/energy.md",
            "tutorials/aoe.md",
        ],
        "API" => "api.md",
        "LICENSE" => "LICENSE.md",
    ],
    doctest = ("fixdoctests" in ARGS) ? :fix : true,
    linkcheck = !("nonstrict" in ARGS),
    warnonly = ("nonstrict" in ARGS),
)

deploydocs(
    repo = "github.com/legend-exp/LegendDataManagement.jl.git",
    forcepush = true,
    push_preview = true,
)
