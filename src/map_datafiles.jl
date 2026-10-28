# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

# ToDo: Parallelize over processes.

"""
    map_datafiles(
        f_process, f_open, data::LegendData,
        category::DataCategoryLike, filekeys::AbstractVector{<:FileKey}
    )

Processes the files of all `filekeys` in `data`.

Opens each file using `f_open`, processes it using `f_process` and closes it.
The file name is `data.tier[category, filekey]`, so `category` has to be a
value that `data.tier` accepts as its first index. Errors while opening or
processing a file are logged and recorded instead of thrown.

Returns a
`@NamedTuple{result::Dict{FileKey}, success::Bool, failed::Dict{FileKey}}`:

* `result` contains the results `f_process(f_open(filename))` for all
  `filekeys` that were processed.
* `failed` contains the error for each file key where opening or processing
  failed.
* `success` equals `isempty(failed)`.

## Arguments
* `f_process`: Function that processes the opened file.
* `f_open`: Function that opens a file name and returns an object that can be closed.
* `data::LegendData`: The data that holds the files.
* `category::DataCategoryLike`: Used as first index of `data.tier`.
* `filekeys::AbstractVector{<:FileKey}`: File keys of the files to process.
"""
function map_datafiles(
    f_process, f_open, data::LegendData,
    category::DataCategoryLike, filekeys::AbstractVector{<:FileKey}
)
    result = Dict{FileKey,Any}()
    failed = Dict{FileKey,Any}()
    @showprogress desc="Processing files" for filekey in filekeys
        filename = data.tier[category, filekey]
        try
            input = f_open(filename)
            try
                result[filekey] = f_process(input)
            catch err
                @error "Failed to process file \"$filename\" due to $(nameof(typeof(err)))"
                failed[filekey] = err
            finally
                close(input)                
            end
        catch err
            @error "Failed to open file \"$filename\" using $f_open"
            failed[filekey] = err
        end
    end

    typed_result = try
        R = typeof(first(values(result)))
        Dict{FileKey,R}(result)
    catch
        result
    end

    return (result = typed_result, success = isempty(failed), failed = failed)
end
export map_datafiles
