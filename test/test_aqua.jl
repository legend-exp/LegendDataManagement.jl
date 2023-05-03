# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

import Test
import Aqua
import LegendDataManagement

Test.@testset "Aqua tests" begin
    Aqua.test_all(LegendDataManagement)
end # testset
