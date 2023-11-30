# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).

# ToDo: Parallelize over processes.

"""
    map_datafiles(
        f_process, f_open, data::LegendData,
        category::DataCategoryLike, filekeys::AbstractVector{<:FileKey}
    )

Processes all `filekeys` in `data` for `category`.

Opens the files using `f_open` and processes them using `f_process`.

Returns a
`@NamedTuple{result::Dict{Filekey}, failed::Dict{Filekey}, success::Bool}`:

* `values(result)` contains the results `f_process(f_open(filename))` for
   all filenames referred to by `category` and `filekeys`.
* `values(result)` contains the error where processing failed.
* `success` equals `isempty(failed)`
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
