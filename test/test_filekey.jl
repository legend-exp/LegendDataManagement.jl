# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).

using GERDAMetadata
using Test

@testset "filekey" begin
    key = FileKey("gerda-run0068-20160718T064953Z-phy")
    @test string(key) == "gerda-run0068-20160718T064953Z-phy"

    @test occursin(key, "%-run0068-%-phy")
    @test !occursin(key, "%-run0069-%-phy")

    @test FileKey("gerda-run0068-20160718T064953Z-phy") == key
end
