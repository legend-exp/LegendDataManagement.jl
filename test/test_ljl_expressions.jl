# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using PropertyFunctions, StructArrays

include("testing_utils.jl")

@testset "legend_expressions" begin
    data = StructArray([
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = 328.2, force_accept = false),
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = NaN, force_accept = false),
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = NaN, force_accept = true),
    ])

    bool_expr_string = "E_trap > 0 && !isnan(A) && !isinf(E_trap) || force_accept"
    bool_expr = parse_ljlexpr(bool_expr_string)
    @test bool_expr == :(E_trap > 0 && (!(isnan(A)) && !(isinf(E_trap))) || force_accept)
    ref_boolfunc(x) = (x.E_trap > 0 && !(isnan(x.A)) && !(isinf(x.E_trap)) || x.force_accept)
    bool_pf = ljl_propfunc(bool_expr)
    @test bool_pf isa PropertyFunctions.PropertyFunction
    @test bool_pf === ljl_propfunc(bool_expr_string)
    @test @inferred(broadcast(bool_pf, data)) == ref_boolfunc.(data)

    num_expr_string = "(offs + slope * abs(E_trap) / 1000) - 5.2"
    num_expr = parse_ljlexpr(num_expr_string)
    @test num_expr == :((offs + (slope * abs(E_trap)) / 1000) - 5.2)
    ref_numfunc(x) = (x.offs + (x.slope * abs(x.E_trap)) / 1000) - 5.2
    num_pf = ljl_propfunc(num_expr)
    @test num_pf isa PropertyFunctions.PropertyFunction
    @test num_pf === ljl_propfunc(num_expr_string)
    @test @inferred(broadcast(num_pf, data)) == ref_numfunc.(data)

    expr_map = props = PropDict(:e_flag => bool_expr_string, :e_cal => num_expr_string)
    multi_pf = ljl_propfunc(expr_map)
    @test multi_pf isa PropertyFunctions.PropertyFunction
    @test @inferred(broadcast(multi_pf, data)) == StructArray(e_cal = ref_numfunc.(data), e_flag = ref_boolfunc.(data))
end
