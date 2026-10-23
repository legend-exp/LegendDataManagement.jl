"""
    get_peaksfilename(data::LegendData, setup::ExpSetupLike, period::DataPeriodLike, run::DataRunLike, category::DataCategoryLike, ch::ChannelIdLike)
    get_peaksfilename(data::LegendData, filekey::FileKey, ch::ChannelIdLike)

Deprecated. Returns the file name of the `peaks` tier data of a channel; use
`data.tier[:peaks, category, period, run, ch]` instead.
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

Deprecated. Returns the file name of the `jlhit` tier data of a channel; use
`data.tier[:jlhit, category, period, run, ch]` instead.
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

Returns the file name of the machine learning training data.

The name is `data.tier[:jlqml, filekey]`, where `filekey` is the start file key
of `category` in `period` and in the run whose number is that of the first
analysis run of period `p03`. For a `FileKey` the period and the category of
the file key are used.
"""
function get_mltrainfilename end
export get_mltrainfilename
function get_mltrainfilename(data::LegendData, period::DataPeriodLike, category::DataCategoryLike)
    first_run = first(sort(filter(x -> x.period == DataPeriod(3), analysis_runs(data)).run, by=x->x.no))
    fk = start_filekey(data, (period, first_run, category))
    data.tier[:jlqml, fk]
end
get_mltrainfilename(data::LegendData, filekey::FileKey) = get_mltrainfilename(data, filekey.period, filekey.category)



"""
    get_partition_channelinfo(data::LegendData, chinfo::Table, period::DataPeriodLike, category::DataCategoryLike; unfold_partitions::Bool = false)

Deprecated. Use `channelinfo(data, period, category; explode_partitions)`
instead.
"""
function get_partition_channelinfo(data::LegendData, chinfo::Table, period::DataPeriodLike, category::DataCategoryLike; unfold_partitions::Bool=false)
    Base.depwarn("`get_partition_channelinfo(data, chinfo, period, category; unfold_partitions)` is deprecated, " *
        "use `channelinfo(data, period, category; explode_partitions)` instead.", :get_partition_channelinfo)
    partition = Vector{DataPartition}[partitioninfo(data, det, category, DataPeriod(period)) for det in chinfo.detector]
    t = Table(merge(columns(chinfo), (; partition)))
    return unfold_partitions ? _explode_partitions(t) : t
end
export get_partition_channelinfo