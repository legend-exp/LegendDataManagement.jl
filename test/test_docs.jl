# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using Test
using LegendDataManagement
import Documenter

Documenter.DocMeta.setdocmeta!(
    LegendDataManagement,
    :DocTestSetup,
    :(using LegendDataManagement);
    recursive=true,
)
Documenter.doctest(LegendDataManagement)
