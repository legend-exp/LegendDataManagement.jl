"""
    get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) 
Get the filename for the peaks data for a given channel.
"""
function get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    Base.depwarn(
        "`get_peaksfilename(data, setup, period, run, category, ch)` is deprecated, use `l200.tier[:peaks, category, period, run, ch]` instead`.",
        ((Base.Core).Typeof(get_peaksfilename)).name.mt.name, force=true
    )
    # joinpath(data.tier[:peaks, :cal, period, run], format("{}-{}-{}-{}-{}-tier_peaks.lh5", string(setup), string(period), string(run), string(category), string(ch)))
    data.tier[:peaks, category, period, run, ch]
end
export get_peaksfilename
get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_peaksfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)

"""
    get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike)
Get the filename for the hitch data for a given channel.
"""
function get_hitchfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    Base.depwarn(
        "`get_hitchfilename(data, setup, period, run, category, ch)` is deprecated, use `l200.tier[:jlhit, category, period, run, ch]` instead`.",
        ((Base.Core).Typeof(get_hitchfilename)).name.mt.name, force=true
    )
    # joinpath(data.tier[:jlhit, category, period, run], format("{}-{}-{}-{}-{}-tier_jlhit.lh5", string(setup), string(period), string(run), string(category), string(ch)))
    data.tier[:jlhit, category, period, run, ch]
end
export get_hitchfilename

get_hitchfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike) = get_hitchfilename(data, filekey.setup, filekey.period, filekey.run, filekey.category, ch)


"""
    get_mltrainfilename(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)
    get_mltrainfilename(data::LegendData, filekey::FileKey)
Get the filename for the machine learning training data.
"""
function get_mltrainfilename end
export get_mltrainfilename
function get_mltrainfilename(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)
    first_run = first(sort(filter(x -> x.period == DataPeriod(3), analysis_runs(data)).run, by=x->x.no))
    fk = start_filekey(data, (period, first_run, category))
    data.tier[:jlml, fk]
end
get_mltrainfilename(data::LegendData, filekey::FileKey) = get_mltrainfilename(data, filekey.period, filekey.category)



function get_partition_channelinfo(data::LegendData, chinfo::Table, period::DataPeriodLike; unfold_partitions::Bool=false)
    # get partition information for given period
    period = DataPeriod(period)
    # get partition information for given period and channels
    parts = partitioninfo.(data, chinfo.channel, period)
    t = StructArray(merge((partition = parts, ), columns(chinfo)))
    if unfold_partitions
        t_unfold = t |> filterby(@pf length($partition) > 1)
        t = t |> filterby(@pf length($partition) == 1)
        t = StructArray(merge(columns(t), (partition = only.(t.partition), )))
        append!(t, [merge(t_ch, (partition = p, )) for t_ch in t_unfold for p in t_ch.partition])
    end
    return Table(t)
end
export get_partition_channelinfo