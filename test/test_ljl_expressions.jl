# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using PropertyFunctions, StructArrays
import Measurements

using PropDicts: PropDict

include("testing_utils.jl")

@testset "legend_expressions" begin
    data = StructArray([
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = 328.2, force_accept = false, a = (b = [1,41,3], c = 51.1)),
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = NaN, force_accept = false, a = (b = [1,42,3], c = 32.0)),
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = NaN, force_accept = true, a = (b = [1,41,3], c = 51.1)),
        (E_trap = 21092, offs = 0.01, slope = 0.73, A = NaN, force_accept = true, a = (b = [1,42,3], c = 32.03)),
    ])

    bool_expr_string = "E_trap > 0 && !isnan(A) && !isinf(E_trap) || force_accept && a.b[2] > a.c"
    bool_expr = parse_ljlexpr(bool_expr_string)
    @test bool_expr == :(E_trap > 0 && (!(isnan(A)) && !(isinf(E_trap))) || force_accept && a.b[2] > a.c)
    ref_boolfunc(x) = (x.E_trap > 0 && !(isnan(x.A)) && !(isinf(x.E_trap)) || x.force_accept && x.a.b[2] > x.a.c)
    bool_pf = ljl_propfunc(bool_expr)
    @test bool_pf isa PropertyFunctions.PropertyFunction
    @test bool_pf === ljl_propfunc(bool_expr_string)
    @test @inferred(broadcast(bool_pf, data)) == ref_boolfunc.(data)

    num_expr_string = "(offs + slope * abs(E_trap) / 1000) - 5.2 + a.b[2]/100 * a.c/50"
    num_expr = parse_ljlexpr(num_expr_string)
    @test num_expr == :((offs + (slope * abs(E_trap)) / 1000) - 5.2 + a.b[2]/100 * a.c/50)
    ref_numfunc(x) = (x.offs + (x.slope * abs(x.E_trap)) / 1000) - 5.2 + x.a.b[2]/100 * x.a.c/50
    num_pf = ljl_propfunc(num_expr)
    @test num_pf isa PropertyFunctions.PropertyFunction
    @test num_pf === ljl_propfunc(num_expr_string)
    @test @inferred(broadcast(num_pf, data)) == ref_numfunc.(data)

    meas_expr_string = "(offs + (slope > 0.5 ? one(slope) : zero(slope)) * abs(E_trap) / 1000) - (5.2 ± 0.1)"
    meas_expr = parse_ljlexpr(meas_expr_string)
    @test meas_expr == :((offs + ((slope > 0.5 ? one(slope) : zero(slope)) * abs(E_trap)) / 1000) - (5.2 ± 0.1))
    ref_measfunc(x) = (x.offs + ((x.slope > 0.5 ? one(x.slope) : zero(x.slope)) * abs(x.E_trap)) / 1000) - Measurements.:(±)(5.2, 0.1)
    meas_pf = ljl_propfunc(meas_expr)
    @test meas_pf isa PropertyFunctions.PropertyFunction
    @test meas_pf === ljl_propfunc(meas_expr_string)
    @test @inferred(broadcast(meas_pf, data)) == ref_measfunc.(data)
 
    expr_map = props = PropDict(:e_flag => bool_expr_string, :e_cal => num_expr_string)
    multi_pf = ljl_propfunc(expr_map)
    @test multi_pf isa PropertyFunctions.PropertyFunction
    @test @inferred(broadcast(multi_pf, data)) == StructArray(e_cal = ref_numfunc.(data), e_flag = ref_boolfunc.(data))
end
