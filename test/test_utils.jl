# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using LegendDataManagement.LDMUtils
using StructArrays
using Test
using YAML

@testset "writevalidity" begin

    # setup of the testdata
    rows = [
    (period = DataPeriod(3), run = DataRun(0), filekey = FileKey("l200-p03-r000-cal-20230311T235840Z"), validity = "B00000A/calpartition001a.yaml")
    (period = DataPeriod(3), run = DataRun(1), filekey = FileKey("l200-p03-r001-cal-20230317T211819Z"), validity = "B00000A/calpartition001a.yaml")
    (period = DataPeriod(3), run = DataRun(2), filekey = FileKey("l200-p03-r002-cal-20230324T161401Z"), validity = "B00000A/calpartition001a.yaml")
    (period = DataPeriod(3), run = DataRun(3), filekey = FileKey("l200-p03-r003-cal-20230331T161141Z"), validity = "B00000A/calpartition001a.yaml")
    (period = DataPeriod(4), run = DataRun(0), filekey = FileKey("l200-p04-r000-cal-20230414T215158Z"), validity = "B00000A/calpartition001a.yaml")

    (period = DataPeriod(3), run = DataRun(1), filekey = FileKey("l200-p03-r001-cal-20230317T211819Z"), validity = "C00ANG5/calpartition001a.yaml")
    (period = DataPeriod(3), run = DataRun(2), filekey = FileKey("l200-p03-r002-cal-20230324T161401Z"), validity = "C00ANG5/calpartition001b.yaml")
    (period = DataPeriod(3), run = DataRun(3), filekey = FileKey("l200-p03-r003-cal-20230331T161141Z"), validity = "C00ANG5/calpartition001c.yaml")
    (period = DataPeriod(4), run = DataRun(0), filekey = FileKey("l200-p04-r000-cal-20230414T215158Z"), validity = "C00ANG5/calpartition001c.yaml")

    (period = DataPeriod(3), run = DataRun(0), filekey = FileKey("l200-p03-r000-cal-20230311T235840Z"), validity = "P00662B/calpartition001a.yaml")
    (period = DataPeriod(3), run = DataRun(1), filekey = FileKey("l200-p03-r001-cal-20230317T211819Z"), validity = "P00662B/calpartition001a.yaml")
    (period = DataPeriod(3), run = DataRun(2), filekey = FileKey("l200-p03-r002-cal-20230324T161401Z"), validity = "P00662B/calpartition001b.yaml")
    (period = DataPeriod(4), run = DataRun(0), filekey = FileKey("l200-p04-r000-cal-20230414T215158Z"), validity = "P00662B/calpartition001b.yaml")
    ]
    rows2 = [
    (period = DataPeriod(4), run = DataRun(0), filekey = FileKey("l200-p04-r000-cal-20230414T215158Z"), validity = "B00000A/calpartition001b.yaml"),
    ]
    validity_1 = (result = StructArray(rows), skipped = false)
    validity_2 = (result = StructArray(rows2), skipped = true)

    @testset "writevalidity_full" begin
        mktempdir() do tmpdir # temporary directory to create validity 
            # construnct temporary PropDict
            props_db = LegendDataManagement.PropsDB{Nothing}(
                tmpdir,
                "",
                String[],
                nothing,
                Symbol[],
                false
            )

            # creating the valdity
            writevalidity(props_db, validity_1)
            writevalidity(props_db, validity_2)

            files = filter(f -> endswith(f, ".yaml"), readdir(tmpdir; join=true))
            @test length(files) == 1

            got = YAML.load_file(files[1])

            # expected validity fily content
            expected = [
                Dict(
                    "valid_from" => "20230311T235840Z",
                    "apply"      => ["B00000A/calpartition001a.yaml",
                                    "C00ANG5/calpartition001a.yaml",
                                    "P00662B/calpartition001a.yaml"],
                    "category"   => "all",
                    "mode"       => "reset"
                ),
                Dict(
                    "valid_from" => "20230324T161401Z",
                    "apply"      => ["C00ANG5/calpartition001a.yaml",
                                    "P00662B/calpartition001a.yaml"],
                    "category"   => "all",
                    "mode"       => "remove"
                ),
                Dict(
                    "valid_from" => "20230324T161401Z",
                    "apply"      => ["C00ANG5/calpartition001b.yaml",
                                    "P00662B/calpartition001b.yaml"],
                    "category"   => "all",
                    "mode"       => "append"
                ),
                Dict(
                    "valid_from" => "20230331T161141Z",
                    "apply"      => ["C00ANG5/calpartition001b.yaml",
                                    "C00ANG5/calpartition001c.yaml"],
                    "category"   => "all",
                    "mode"       => "replace"
                ),
                Dict(
                    "valid_from" => "20230414T215158Z",
                    "apply"      => ["B00000A/calpartition001a.yaml",
                                    "B00000A/calpartition001b.yaml"],
                    "category"   => "all",
                    "mode"       => "replace"
                )
            ]

            normalize_entry(d) = Dict(k => (v isa Vector ? sort(v) : v) for (k,v) in d)    
            @test map(normalize_entry, got) == map(normalize_entry, expected)
        end
    end


    @testset "writevalidity_diff" begin
        mktempdir() do tmpdir # temporary directory to create validity 
            # construnct temporary PropDict
            props_db = LegendDataManagement.PropsDB{Nothing}(
                tmpdir,
                "",
                String[],
                nothing,
                Symbol[],
                false
            )

                # creating the valdity
            writevalidity(props_db, validity_1; impl = :diff)
            writevalidity(props_db, validity_2; impl = :diff)

            files = filter(f -> endswith(f, ".yaml"), readdir(tmpdir; join=true))
            @test length(files) == 1

            got = YAML.load_file(files[1])


            # expected validity fily content
            expected = [
                Dict(
                    "valid_from" => "20230311T235840Z",
                    "apply"      => ["B00000A/calpartition001a.yaml",
                                    "P00662B/calpartition001a.yaml"],
                    "category"   => "all",
                    "mode"       => "reset"
                ),
                Dict(
                    "valid_from" => "20230317T211819Z",
                    "apply"      => ["C00ANG5/calpartition001a.yaml"],
                    "category"   => "all",
                    "mode"       => "append"
                ),
                Dict(
                    "valid_from" => "20230324T161401Z",
                    "apply"      => ["C00ANG5/calpartition001a.yaml",
                                    "P00662B/calpartition001a.yaml"],
                    "category"   => "all",
                    "mode"       => "remove"
                ),
                Dict(
                    "valid_from" => "20230324T161401Z",
                    "apply"      => ["C00ANG5/calpartition001b.yaml",
                                    "P00662B/calpartition001b.yaml"],
                    "category"   => "all",
                    "mode"       => "append"
                ),
                Dict(
                    "valid_from" => "20230331T161141Z",
                    "apply"      => ["C00ANG5/calpartition001b.yaml",
                                    "P00662B/calpartition001b.yaml"],
                    "category"   => "all",
                    "mode"       => "remove"
                ),
                Dict(
                    "valid_from" => "20230331T161141Z",
                    "apply"      => ["C00ANG5/calpartition001c.yaml"],
                    "category"   => "all",
                    "mode"       => "append"
                ),
                Dict(
                    "valid_from" => "20230414T215158Z",
                    "apply"      => ["B00000A/calpartition001b.yaml"],
                    "category"   => "all",
                    "mode"       => "append"
                )
            ]


            normalize_entry(d) = Dict(k => (v isa Vector ? sort(v) : v) for (k,v) in d)    
            @test map(normalize_entry, got) == map(normalize_entry, expected)
        end
    end
end