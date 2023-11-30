# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using PropertyFunctions

@testset "legend_expressions" begin
    data = StructArray([
        (trapTmax = 120.3, trapSmax = 5, cuspEmax_ctc_cal = 44.0, is_saturated = false),
        (trapTmax = 120.3, trapSmax = 5, cuspEmax_ctc_cal = NaN, is_saturated = false),
        (trapTmax = 120.3, trapSmax = 5, cuspEmax_ctc_cal = 44.0, is_saturated = true),
    ])

    lpy_expr_string = "((trapTmax-trapSmax)>-100)&((trapTmax-trapSmax)<100)|(cuspEmax_ctc_cal<25)|(cuspEmax_ctc_cal!=cuspEmax_ctc_cal)|(is_saturated)"
    ref_jl_expr_string = "((trapTmax - trapSmax > -100 && trapTmax - trapSmax < 100 || cuspEmax_ctc_cal < 25) || cuspEmax_ctc_cal != cuspEmax_ctc_cal) || is_saturated"
    expr = parse_lpyexpr(lpy_expr_string)
    @test expr isa LJlExprLike
    @test string(parse_lpyexpr(lpy_expr_string)) == ref_jl_expr_string
    pf = lpy_propfunc(lpy_expr_string)
    @test pf isa PropertyFunctions.PropertyFunction
    @test @inferred(broadcast(pf, data)) == [false, true, true]
end
