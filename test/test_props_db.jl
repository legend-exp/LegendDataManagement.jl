# This file is a part of jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates
using PropDicts

using LegendDataManagement: PropsDB, AnyProps, ValiditySelection

include("testing_utils.jl")

@testset "props_db" begin
    props_base_path = data_path(LegendDataConfig().setups.l200, "metadata")

    @test AnyProps(props_base_path) isa PropsDB
    @test_throws ArgumentError AnyProps("/no/such/props/db")

    pd = AnyProps(props_base_path)

    filekey = FileKey("l200-p02-r006-cal-20221226T200846Z")
    @test pd(filekey) isa PropsDB{ValiditySelection}
    @test pd("20221226T200846Z", :all) isa PropsDB{ValiditySelection}

    @test pd.hardware.configuration.channelmaps isa PropsDB
    @test pd.hardware.configuration.channelmaps(filekey) isa PropDict
    @test pd.hardware(filekey).configuration.channelmaps isa PropDict

    channelmaps = pd.hardware.configuration.channelmaps
    filelist = LegendDataManagement._validity_sel_filelist(channelmaps, filekey)
    @test filelist == ["l200-p02-r%-T%-all-config.yaml", "l200-p02-r%-T%-cal-config.yaml", "l200-p02-r009-T%-all-config.yaml"]
    @test LegendDataManagement._validity_sel_filelist(channelmaps, FileKey("l200-p02-r006-cal-20221226T210000Z")) == filelist
    @test LegendDataManagement._validity_sel_filelist(channelmaps, FileKey("l200-p03-r000-cal-20230311T235840Z")) == ["l200-p03-r%-T%-all-config.yaml"]

    # a run override selects the validity of the filekey it applies, an empty apply ends it
    override_file = tempname() * ".yaml"
    write(override_file, """
    - valid_from: 20221226T200000Z
      apply:
        - l200-p03-r000-cal-20230311T235840Z
    - valid_from: 20221226T210000Z
      mode: reset
      apply: []
    """)
    run_override = LegendDataManagement._load_validity(override_file)
    pd_override = AnyProps(props_base_path; run_override)
    channelmaps_override = pd_override.hardware.configuration.channelmaps
    @test LegendDataManagement._validity_sel_filelist(channelmaps_override, filekey) == ["l200-p03-r%-T%-all-config.yaml"]
    @test channelmaps_override(filekey) == channelmaps(FileKey("l200-p03-r000-cal-20230311T235840Z"))
    @test LegendDataManagement._validity_sel_filelist(channelmaps_override, FileKey("l200-p02-r006-cal-20221226T220000Z")) == filelist
end
