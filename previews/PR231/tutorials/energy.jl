using LegendDataManagement
using LegendHDF5IO
using PropDicts
using PropertyFunctions
using TypedTables
using Unitful
using Measurements

l200 = LegendData(:l200);

rinfo = runinfo(l200, DataPeriod(2))
rinfo.run

period, run = DataPeriod(2), DataRun(6)
filekey = start_filekey(l200, (period, run, :cal))

chinfo = channelinfo(l200, filekey; system = :geds, only_processable = true)
chinfo_icpc = chinfo |> filterby(@pf $det_type == :icpc && $usability == :on)
chinfo_icpc.detector

chinfo_det = channelinfo(l200, filekey, DetectorId(:V99000A))
(chinfo_det.detector, chinfo_det.channel, chinfo_det.det_type)

dsp = read_ldata(l200, :jldsp, fk, det)
length(dsp), length(propertynames(dsp))

data_all = read_ldata((:e_cusp, :qdrift), l200, :jldsp, fk, det)
data_all[1:3]

mask = read_ldata(@pf($e_cusp > 10000.0), l200, :jldsp, fk, det)
data = data_all[mask]
length(data)

ctc_func = "e_cusp .- 0.0001 .* qdrift"
e_ctc = ljl_propfunc(ctc_func).(data)
first(e_ctc, 3)

ecal_func = "-0.5keV .* ($ctc_func).^0 .+ 0.0037keV .* ($ctc_func).^1"
pars_ecal = PropDict(
    Symbol(det) => PropDict(
        :e_cusp_ctc => PropDict(
            :m_cal_simple => (0.0037 ± 0.0001) * u"keV",
            :cal => PropDict(:func => ecal_func),
        ),
    ),
)
ecal_file = joinpath(scratch_dir, "ecal.yaml")
writelprops(ecal_file, pars_ecal)
print(read(ecal_file, String))

pars_ecal_read = readlprops(ecal_file)
pars_ecal_read[det].e_cusp_ctc.m_cal_simple

cal_func = ljl_propfunc(pars_ecal_read[det].e_cusp_ctc.cal.func)
e_cal = cal_func.(data_all)
first(e_cal, 3)
