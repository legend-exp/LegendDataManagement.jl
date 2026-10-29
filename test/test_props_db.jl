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

@testset "merge validity" begin
    t1, t2, t3 = Timestamp.(["20230101T000000Z", "20230201T000000Z", "20230301T000000Z"])
    cat = DataCategory(:all)
    primary = LegendDataManagement._ValidityDict(cat => (valid_from = [t1, t3], filelist = [["a1.yaml"], ["a3.yaml"]]))
    override = LegendDataManagement._ValidityDict(cat => (valid_from = [t2], filelist = [["o.yaml"]]))
    merged = LegendDataManagement._merge_validity_info!!(primary, override)[cat]
    # an override entry keeps the files of the primary list that are active at its time, and vice versa
    @test merged.valid_from == [t1, t2, t3]
    @test merged.filelist == [["a1.yaml"], ["a1.yaml", "o.yaml"], ["a3.yaml", "o.yaml"]]

    # a category that only one list has is merged with the entries for category all of the other list
    sel_files(validity, category) = LegendDataManagement._get_validity_sel_filelist(validity, ValiditySelection(t2, category))
    primary = LegendDataManagement._ValidityDict(cat => (valid_from = [t1], filelist = [["a.yaml"]]))
    override = LegendDataManagement._ValidityDict(DataCategory(:cal) => (valid_from = [t2], filelist = [["o-cal.yaml"]]))
    merged = LegendDataManagement._merge_validity_info!!(primary, override)
    @test sel_files(merged, :cal) == ["a.yaml", "o-cal.yaml"]
    @test sel_files(merged, :phy) == ["a.yaml"]
    primary = LegendDataManagement._ValidityDict(DataCategory(:cal) => (valid_from = [t1], filelist = [["a-cal.yaml"]]), DataCategory(:phy) => (valid_from = [t1], filelist = [["a-phy.yaml"]]))
    override = LegendDataManagement._ValidityDict(cat => (valid_from = [t2], filelist = [["o.yaml"]]))
    merged = LegendDataManagement._merge_validity_info!!(primary, override)
    @test sel_files(merged, :cal) == ["a-cal.yaml", "o.yaml"]
    @test sel_files(merged, :phy) == ["a-phy.yaml", "o.yaml"]
    @test sel_files(merged, :ssc) == ["o.yaml"]
end
