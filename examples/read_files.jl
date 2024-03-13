# simple example script to read lh5 files with Julia software
# for testing or as reference
using LegendHDF5IO, HDF5             # LEGEND module for data I/o,
using LegendDataManagement      # handles data path and metadata 
using TypedTables
using Plots
using Formatting

l200 = LegendData(:l200);              # create LegendData object of LegendDataManagement module
@info "Loading Legend MetaData Object" # print in nice format to REPL

# select data: some meta data
mode = :cal
periods  = search_disk(DataPeriod,l200.tier[:jldsp,mode])# periods for which dsp files exists
runs    = search_disk(DataRun,l200.tier[:jldsp,mode,periods_dsp[1]]) # runs within selected period 
filekeys  = search_disk(FileKey,l200.tier[:jldsp,mode,periods_dsp[1],runs_dsp[1]]) # first existing filekey
filekey_start   = start_filekey(l200,(periods[1], runs[1], :cal)) # alternative to search_disk(Filekey,...), indepdendent of dsp, raw etc. type 

# get channel/detector information 
ch_ged   = channelinfo(l200,filekey_start,system = :geds, only_processable = true).channel # Germanium detector channels 
dets_ged = channelinfo(l200,filekey_start,system = :geds, only_processable = true).detector # Germanium detector names

# 1. raw file: open  and look at waveform 
# data = lh5open(l200.tier[:raw,filekey_start],"r") # open raw file and load into memory. all channels -> many waveforms -> takes long
data_raw = lh5open(l200.tier[:raw,filekey_start],"r")["$(ch_ged[1])/raw"][1:10] # load only 1 channel and 10 waveforms
Table(data_raw) # look what is inside
columnnames(data_raw) # list of keys 
wvf = data_raw.waveform # get waveform data
plot(wvf[1]) # plot 1 waveform

# 2. dsp files
data_dsp = lh5open(l200.tier[:jldsp,filekey_start],"r") # read dsp parameter: 1 file, all channels
data_dsp[ch_ged[1]].blmean #access some parameter of a channel, here: baseline mean
histogram(data_dsp[ch_ged[1]].blmean) # look at distribution 
Table(data_dsp[ch_ged[1]]) #overview of all parameters of 1 channel

# 3. hitch files: 1 file per run per channel 
get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike) = joinpath(data.tier[:jlhitch, category, period, run], format("{}-{}-{}-{}-{}-tier_jlhit.lh5", string(setup), string(period), string(run), string(category), string(ch)))
get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_hitchfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)
file_hitch = get_hitchfilename(l200,filekey_start,ch_ged[1]) # get file name
#path_hitch = l200.tier[:jlhitch,:cal,periods[1],runs[1]] 
data_hitch = lh5open(file_hitch,"r")["$(ch_geds[1])/dataQC"] # read hitch data for 1 channel 

# 4. event files
filekey_phy_start   = start_filekey(l200,(periods[1], runs[1], :phy)) # alternative to search_disk(Filekey,...), indepdendent of dsp, raw etc. type 
data_evt   = lh5open(l200.tier[:jlevt,filekey_start],"r")

# 4. read parameters: fit of calibration spectra, calibration curves, 
keys(l200.par) # :ppars -> partition , :rpars - > run pars 
keys(l200.par.rpars) # all kinds of parameters 
keys(l200.par.rpars.ecal.p03.r000.P00664A) # energy-related parameters of period 3, run 0, detector P00664A
fwhm = l200.par.rpars.ecal.p03.r000.P00664A.e_cusp_ctc.fit.:Tl208FEP.fwhm # example: fwhm for calibration fits  of Tl208FEP line using e_cusp_ctc and detector P00664mA



