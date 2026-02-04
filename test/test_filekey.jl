# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

using LegendDataManagement
using Test

using Dates
using Unitful

@testset "filekey" begin
    setup = ExpSetup(:l200)
    @test setup.label == :l200
    @test @inferred(string(setup)) == "l200"
    @test @inferred(ExpSetup("l200")) == setup
    @test_throws ArgumentError DataPeriod("invalidsetup")

    period = DataPeriod(2)
    @test period.no == 2
    @test @inferred(string(period)) == "p02"
    @test @inferred(DataPeriod("p02")) == period
    @test_throws ArgumentError DataPeriod("invalidperiod")
    # test broadcasting of DataSelector
    periods = [DataPeriod(1), DataPeriod(2), DataPeriod(3)]
    @test (periods .== period) == [false, true, false]

    r = DataRun(6)
    @test r.no == 6
    @test @inferred(string(r)) == "r006"
    @test @inferred(DataRun("r006")) == r
    @test_throws ArgumentError DataRun("invalidrun")

    category = DataCategory(:cal)
    @test category.label == :cal
    @test @inferred(string(category)) == "cal"
    @test @inferred(DataCategory("cal")) == category
    @test_throws ArgumentError DataCategory("invalidstring")

    p = DataPartition("calgroup001a")
    @test p.no == 1
    @test p.set == :a
    @test p.cat == DataCategory(:cal)
    @test string(p) == "calpartition001a"
    @test p == DataPartition(001)
    @test p < DataPartition(:calgroup002)
    @test_throws ArgumentError DataPartition("invalidstring")

    timestamp = @inferred(Timestamp("20221226T200846Z"))
    @test timestamp.unixtime == 1672085326
    @test @inferred(string(timestamp)) == "20221226T200846Z"

    unix_timestamp = 1672085326u"s"
    timestamp2 = @inferred(Timestamp(unix_timestamp))
    @test timestamp2.unixtime == 1672085326
    @test @inferred(string(timestamp2)) == "20221226T200846Z"
    
    key = @inferred FileKey("l200-p02-r006-cal-20221226T200846Z")
    @test string(key) == "l200-p02-r006-cal-20221226T200846Z"

    # @test occursin(key, "%-r006-%-phy")
    # @test !occursin(key, "%-r006-%-phy")

    @test FileKey("l200-p02-r006-cal-20221226T200846Z") == key
    @test @inferred(FileKey("tier/raw/cal/p02/r006/l200-p02-r006-cal-20221226T200846Z-tier_raw.lh5")) == key
    @test @inferred(FileKey("l200", "p02", "r006", "cal", "20221226T200846Z")) == key

    @test @inferred(LegendDataManagement._is_filekey_string("l200-p02-r006-cal-20221226T200846Z")) == true
    @test @inferred(LegendDataManagement._is_filekey_string("20221226T200846Z")) == false

    @test @inferred(LegendDataManagement._is_timestamp_string("20221226T200846Z")) == true
    @test @inferred(LegendDataManagement._is_timestamp_string("20221226200846")) == false

    @test @inferred(LegendDataManagement._timestamp_from_string("l200-p02-r006-cal-20221226T200846Z")) == DateTime("2022-12-26T20:08:46")
    @test @inferred(LegendDataManagement._timestamp_from_string("20221226T200846Z")) == DateTime("2022-12-26T20:08:46")
    @test_throws ArgumentError LegendDataManagement._timestamp_from_string("20221226200846Z")

    ch = ChannelId(1083204)
    @test ch.no == 1083204
    @test @inferred(string(ch)) == "ch1083204"
    @test @inferred(ChannelId("ch1083204")) == ch

    # Basic DetectorId tests
    detector = DetectorId(:V99000A)
    @test UInt32(detector) isa UInt32
    @test Int(detector) isa Int
    @test @inferred(Symbol(detector)) == :V99000A
    @test @inferred(convert(Symbol, detector)) == :V99000A
    @test @inferred(string(detector)) == "V99000A"
    @test @inferred(DetectorId("V99000A")) == detector
    @test @inferred(DetectorId(:V99000A)) == detector
    @test @inferred(DetectorId(UInt32(detector))) == detector
    @test @inferred(DetectorId(Int(detector))) == detector
    
    # Test Int/UInt32 conversions
    @test Int(detector) == UInt32(detector)
    @test convert(Int, detector) == Int(detector)
    @test convert(UInt32, detector) == UInt32(detector)
    
    # Test construction from various integer types
    int32_val = UInt32(detector)
    @test DetectorId(int32_val) == detector
    @test DetectorId(Int64(int32_val)) == detector
    @test DetectorId(Int(int32_val)) == detector
    @test DetectorId(UInt32(int32_val)) == detector
    
    # Test repr/show
    @test repr(detector) == "DetectorId(\"V99000A\")"

    # Comprehensive DetectorId encoding tests
    @testset "DetectorId encoding" begin
        # HPGe detectors (B, C, P, V)
        @testset "HPGe detectors" begin
            # B type (type code = 0x2)
            @test string(DetectorId("B00000C")) == "B00000C"
            @test UInt32(DetectorId("B00000C")) == UInt32(0x02000002)
            @test DetectorId(UInt32(0x02000002)) == DetectorId("B00000C")
            
            @test string(DetectorId("B59231A")) == "B59231A"
            @test UInt32(DetectorId("B59231A")) == UInt32(0x020e75f0)
            @test DetectorId(UInt32(0x020e75f0)) == DetectorId("B59231A")
            
            # C type (type code = 0x1)
            @test string(DetectorId("C00000A")) == "C00000A"
            @test UInt32(DetectorId("C00000A")) == UInt32(0x01000000)
            @test DetectorId(UInt32(0x01000000)) == DetectorId("C00000A")
            
            @test string(DetectorId("C83847I")) == "C83847I"
            @test UInt32(DetectorId("C83847I")) == UInt32(0x01147878)
            @test DetectorId(UInt32(0x01147878)) == DetectorId("C83847I")
            
            # P type (type code = 0x3)
            @test string(DetectorId("P94752A")) == "P94752A"
            @test UInt32(DetectorId("P94752A")) == UInt32(0x03172200)
            @test DetectorId(UInt32(0x03172200)) == DetectorId("P94752A")
            
            @test string(DetectorId("P00000K")) == "P00000K"
            @test UInt32(DetectorId("P00000K")) == UInt32(0x0300000a)
            @test DetectorId(UInt32(0x0300000a)) == DetectorId("P00000K")
            
            # V type (type code = 0x4)
            @test string(DetectorId("V99999J")) == "V99999J"
            @test UInt32(DetectorId("V99999J")) == UInt32(0x041869f9)
            @test DetectorId(UInt32(0x041869f9)) == DetectorId("V99999J")
            
            @test string(DetectorId("V98237P")) == "V98237P"
            @test UInt32(DetectorId("V98237P")) == UInt32(0x0417fbdf)
            @test DetectorId(UInt32(0x0417fbdf)) == DetectorId("V98237P")
        end
        
        # Special C detectors (C000RG and C00ANG)
        @testset "Special C detectors" begin
            @test string(DetectorId("C000RG4")) == "C000RG4"
            @test UInt32(DetectorId("C000RG4")) == UInt32(0x01f20040)
            @test DetectorId(UInt32(0x01f20040)) == DetectorId("C000RG4")
            
            @test string(DetectorId("C00ANG7")) == "C00ANG7"
            @test UInt32(DetectorId("C00ANG7")) == UInt32(0x01f10070)
            @test DetectorId(UInt32(0x01f10070)) == DetectorId("C00ANG7")
        end
        
        # SiPM detectors (type code = 0x9)
        @testset "SiPM detectors" begin
            @test string(DetectorId("S000")) == "S000"
            @test UInt32(DetectorId("S000")) == UInt32(0x09000000)
            @test DetectorId(UInt32(0x09000000)) == DetectorId("S000")
            
            @test string(DetectorId("S632")) == "S632"
            @test UInt32(DetectorId("S632")) == UInt32(0x09002780)
            @test DetectorId(UInt32(0x09002780)) == DetectorId("S632")
            
            @test string(DetectorId("S999")) == "S999"
            @test UInt32(DetectorId("S999")) == UInt32(0x09003e70)
            @test DetectorId(UInt32(0x09003e70)) == DetectorId("S999")
        end
        
        # PMT detectors (type code = 0xa)
        @testset "PMT detectors" begin
            @test string(DetectorId("PMT000")) == "PMT000"
            @test UInt32(DetectorId("PMT000")) == UInt32(0x0a000000)
            @test DetectorId(UInt32(0x0a000000)) == DetectorId("PMT000")
            
            @test string(DetectorId("PMT183")) == "PMT183"
            @test UInt32(DetectorId("PMT183")) == UInt32(0x0a000b70)
            @test DetectorId(UInt32(0x0a000b70)) == DetectorId("PMT183")
            
            @test string(DetectorId("PMT999")) == "PMT999"
            @test UInt32(DetectorId("PMT999")) == UInt32(0x0a003e70)
            @test DetectorId(UInt32(0x0a003e70)) == DetectorId("PMT999")
        end
        
        # Pulser (type code = 0xb)
        @testset "Pulser detectors" begin
            @test string(DetectorId("PULS00")) == "PULS00"
            @test UInt32(DetectorId("PULS00")) == UInt32(0x0b000000)
            @test DetectorId(UInt32(0x0b000000)) == DetectorId("PULS00")
            
            @test string(DetectorId("PULS00ANA")) == "PULS00ANA"
            @test UInt32(DetectorId("PULS00ANA")) == UInt32(0x0b000001)
            @test DetectorId(UInt32(0x0b000001)) == DetectorId("PULS00ANA")
            
            @test string(DetectorId("PULS99")) == "PULS99"
            @test UInt32(DetectorId("PULS99")) == UInt32(0x0b000630)
            @test DetectorId(UInt32(0x0b000630)) == DetectorId("PULS99")
            
            @test string(DetectorId("PULS99ANA")) == "PULS99ANA"
            @test UInt32(DetectorId("PULS99ANA")) == UInt32(0x0b000631)
            @test DetectorId(UInt32(0x0b000631)) == DetectorId("PULS99ANA")
        end
        
        # AUX (type code = 0xc)
        @testset "AUX detectors" begin
            @test string(DetectorId("AUX00")) == "AUX00"
            @test UInt32(DetectorId("AUX00")) == UInt32(0x0c000000)
            @test DetectorId(UInt32(0x0c000000)) == DetectorId("AUX00")
            
            @test string(DetectorId("AUX99")) == "AUX99"
            @test UInt32(DetectorId("AUX99")) == UInt32(0x0c000630)
            @test DetectorId(UInt32(0x0c000630)) == DetectorId("AUX99")
        end
        
        # DUMMY (type code = 0xd) - including legacy single-digit support
        @testset "DUMMY detectors" begin
            # Legacy single-digit parsing
            @test DetectorId("DUMMY0") == DetectorId("DUMMY00")
            @test DetectorId("DUMMY9") == DetectorId("DUMMY09")
            
            @test string(DetectorId("DUMMY00")) == "DUMMY00"
            @test UInt32(DetectorId("DUMMY00")) == UInt32(0x0d000000)
            @test DetectorId(UInt32(0x0d000000)) == DetectorId("DUMMY00")
            
            @test string(DetectorId("DUMMY09")) == "DUMMY09"
            @test UInt32(DetectorId("DUMMY09")) == UInt32(0x0d000090)
            @test DetectorId(UInt32(0x0d000090)) == DetectorId("DUMMY09")
            
            @test string(DetectorId("DUMMY10")) == "DUMMY10"
            @test UInt32(DetectorId("DUMMY10")) == UInt32(0x0d0000a0)
            @test DetectorId(UInt32(0x0d0000a0)) == DetectorId("DUMMY10")
            
            @test string(DetectorId("DUMMY99")) == "DUMMY99"
            @test UInt32(DetectorId("DUMMY99")) == UInt32(0x0d000630)
            @test DetectorId(UInt32(0x0d000630)) == DetectorId("DUMMY99")
        end
        
        # BSLN (type code = 0xe)
        @testset "BSLN detectors" begin
            @test string(DetectorId("BSLN00")) == "BSLN00"
            @test UInt32(DetectorId("BSLN00")) == UInt32(0x0e000000)
            @test DetectorId(UInt32(0x0e000000)) == DetectorId("BSLN00")
            
            @test string(DetectorId("BSLN99")) == "BSLN99"
            @test UInt32(DetectorId("BSLN99")) == UInt32(0x0e000630)
            @test DetectorId(UInt32(0x0e000630)) == DetectorId("BSLN99")
        end
        
        # MUON (type code = 0xf)
        @testset "MUON detectors" begin
            @test string(DetectorId("MUON00")) == "MUON00"
            @test UInt32(DetectorId("MUON00")) == UInt32(0x0f000000)
            @test DetectorId(UInt32(0x0f000000)) == DetectorId("MUON00")
            
            @test string(DetectorId("MUON99")) == "MUON99"
            @test UInt32(DetectorId("MUON99")) == UInt32(0x0f000630)
            @test DetectorId(UInt32(0x0f000630)) == DetectorId("MUON99")
        end
        
        # Error cases
        @testset "Invalid DetectorId" begin
            @test_throws ArgumentError DetectorId("INVALID")
            @test_throws ArgumentError DetectorId("X00000A")
            @test_throws ArgumentError DetectorId("B0000A")  # Only 4 digits
            @test_throws ArgumentError DetectorId("B000000A")  # 6 digits
            # Sub-serial overflow: Q-Z would overflow the 4-bit field
            @test_throws ArgumentError DetectorId("V99999Q")
            @test_throws ArgumentError DetectorId("V99999Z")
            @test_throws ArgumentError DetectorId("B00000R")
        end
    end
end
