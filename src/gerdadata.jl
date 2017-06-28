# This file is a part of GERDAMetadata.jl, licensed under the MIT License (MIT).

using JSON
using Glob
using PropDicts


const config_file_name_glob = "*-config.json"
const config_file_name_expr = r"^(.*)-config.json$"

const raw_file_suffix = Dict(
    :phy => "",
    :cal => "-calib",
    :pca => "-pcali",
)

const calib_categories = [ :cal, :pca ]


export GERDAData
mutable struct GERDAData
    config::DataConfig
end


raw_data_location(data::GERDAData, setup::Symbol) = data.config.setups[setup].data.raw
export raw_data_location

gen_data_location(data::GERDAData, setup::Symbol) = data.config.setups[setup].data.gen
export gen_data_location

meta_data_location(data::GERDAData, setup::Symbol) = data.config.setups[setup].data.meta
export meta_data_location


function data_dirname(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    runstr = filekey_run_str(key)
    if tier == :tier0
        runstr
    else
        joinpath(string(tier), string(system), string(key.category), runstr)
    end
end
export data_dirname


const tier0_timestamp_format = dateformat"yyyymmdd-HHMMSS"

function data_basename(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    if tier == :tier0
        timestamp = Dates.format(Dates.unix2datetime(key.time), tier0_timestamp_format)
        suffix = raw_file_suffix[key.category]
        "$(timestamp).events$(suffix)"
    else
        "$key-$system-$tier"
    end
end
export data_basename


function data_base_filename(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    data_location = if tier == :tier0
        raw_data_location(data, key.setup)
    else 
        gen_data_location(data, key.setup)
    end

    joinpath(
        data_location,
        data_dirname(data, key, system, tier),
        data_basename(data, key, system, tier)
    )
end
export data_base_filename


function data_filename(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    if tier == :tier0
        data_base_filename(data, key, system, tier) * ".bz2"
    else
        data_base_filename(data, key, system, tier) * ".root"
    end
end
export data_filename


function log_filename(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    if tier == :tier0
        throw(ArgumentError("Can't provide log file names for tier0"))
    else
        data_base_filename(data, key, system, tier) * ".log"
    end
end
export log_filename


function calib_filename(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    if !(key.category in calib_categories)
        throw(ArgumentError("Can't generate calib file path for file key of category \"$(key.category)\""))
    end

    joinpath(
        meta_data_location(data, key.setup),
        "calib",
        filekey_run_str(key),
        "$key-$system-$tier-calib.json"
    )
end
export calib_filename


function calib_catalog_filename(data::GERDAData, setup::Symbol)
    joinpath(
        meta_data_location(data, setup),
        "calib",
        "$setup-calibrations.jsonl"
    )
end
export calib_catalog_filename


function calib_catalog(data::GERDAData, setup::Symbol)
    read(CalibCatalog, calib_catalog_filename(data, setup))
end
export calib_catalog


function calib_available_for(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    calib_available(calib_catalog(data, key.setup), key, system)
end
export calib_available_for


function calib_filename_for(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    calib_key = calibfor(calib_catalog(data, key.setup), key, system)
    calib_filename(data, calib_key, system, tier)
end
export calib_filename_for


function calib_props_for(data::GERDAData, key::FileKey, system::Symbol, tier::Symbol)
    filename = calib_filename_for(data, key, system, tier)
    read(PropDict, filename, subst_pathvar = true, subst_env = true, trim_null = true)
end
export calib_props_for


function config_filenames(data::GERDAData, key::FileKey)
    cfg_files = glob(config_file_name_glob, joinpath(meta_data_location(data, key.setup), "config"))

    function matches_key(filename)
        bname = basename(filename)
        m = match(config_file_name_expr, bname)
        if m != nothing
            ismatch(key, m.captures[1])
        else
            false
        end
    end

    sort([f for f in cfg_files if matches_key(f)])
end
export config_filenames


function config_props_for(data::GERDAData, key::FileKey)
    read(PropDict, config_filenames(data, key), subst_pathvar = true, subst_env = true, trim_null = true)
end
export config_props_for
