# This file is a part of jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates
using PropDicts
using JSON
using Unitful, Measurements

using LegendDataManagement: PropsDB, AnyProps, ValiditySelection

include("testing_utils.jl")

@testset "lprops" begin
    a = """
{
"A00000": {
        "n": 500,
        "a": {
            "val": 1.55,
            "err": 0.01,
            "unit": "μs"
        },
        "b": {
            "val": 15.5,
            "unit": "μs"
        }, 
        "c": {
            "val": 155,
            "err": 1
        }
    }
}
"""
    pd = PropDict(JSON.parse(a))

    @test pd isa PropDict

    pd = LegendDataManagement._props2lprops(pd)

    @test pd.A00000.a isa Unitful.Quantity
    @test ustrip(pd.A00000.a) isa Measurements.Measurement
    @test pd.A00000.b isa Unitful.Quantity
    @test pd.A00000.c isa Measurements.Measurement

    @test PropDict(JSON.parse(a)) == LegendDataManagement._lprops2props(LegendDataManagement._props2lprops(PropDict(JSON.parse(a))))

end
