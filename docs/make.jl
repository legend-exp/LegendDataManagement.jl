# Use
#
#     DOCUMENTER_DEBUG=true julia --color=yes make.jl local [nonstrict] [fixdoctests]
#
# for local builds.

using Documenter
using LegendDataManagement

using LegendTestData
using SolidStateDetectors

# Doctest setup
DocMeta.setdocmeta!(
    LegendDataManagement,
    :DocTestSetup,
    :(using LegendDataManagement);
    recursive=true,
)

makedocs(
    sitename = "LegendDataManagement",
    modules = [LegendDataManagement],
    format = Documenter.HTML(
        prettyurls = !("local" in ARGS),
        canonical = "https://legend-exp.github.io/LegendDataManagement.jl/stable/"
    ),
    pages = [
        "Home" => "index.md",
        "API" => "api.md",
        "Extensions" => "extensions.md",
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
