using LegendDataManagement
using LegendHDF5IO
using PropDicts
using PropertyFunctions
using TypedTables
using Unitful
using Measurements

l200 = LegendData(:l200);

det_meta = DetectorId(:V99000A)
chinfo_det = channelinfo(l200, mfk, det_meta)
(chinfo_det.detector, chinfo_det.low_aoe_status, chinfo_det.partition)

partinfo = partitioninfo(l200, det_meta, chinfo_det.partition)
partinfo.run

ecal_func = "0.0037keV .* (e_cusp .- 0.0001 .* qdrift).^1"
ecal_file = joinpath(scratch_dir, "ecal.yaml")
writelprops(ecal_file, PropDict(Symbol(det) => PropDict(:e_cusp_ctc => PropDict(:cal => PropDict(:func => ecal_func)))))

pars_ecal = readlprops(ecal_file)
e_type = :e_cusp_ctc_cal
e_type_uncal = :e_cusp_ctc
cal_func = ljl_propfunc(pars_ecal[det][e_type_uncal].cal.func);

hit_cal = read_ldata(row -> merge(NamedTuple{(e_type,)}((cal_func(row),)), row), l200, :jldsp, fk, det)
e_cal = getproperty(hit_cal, e_type)
first(e_cal, 3)

aoe_raw_expression = "a_sg / e_cusp"
aoe_raw = ljl_propfunc(aoe_raw_expression).(hit_cal)
first(aoe_raw, 3)

aoe_type = :aoe_sg
aoe_func_string = "($aoe_raw_expression) / 0.0153"
pars_aoe = PropDict(
    Symbol(det) => PropDict(
        aoe_type => PropDict(:func => aoe_func_string, :apply_ctc => false),
        Symbol("$(aoe_type)_classifier") => PropDict(:lowcut => 0.9),
    ),
)
aoe_file = joinpath(scratch_dir, "aoe.yaml")
writelprops(aoe_file, pars_aoe)
print(read(aoe_file, String))

pars_aoe_read = readlprops(aoe_file)
aoe_stored = ljl_propfunc(pars_aoe_read[det][aoe_type].func).(hit_cal)
lowcut = pars_aoe_read[det][Symbol("$(aoe_type)_classifier")].lowcut
pass_cut = aoe_stored .>= lowcut
(count(pass_cut), length(pass_cut))
