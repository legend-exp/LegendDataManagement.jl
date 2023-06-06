# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


"""
    legend_addprocs(nprocs::Integer)

Add Julia workers for LEGEND data processing.

Calls `Distributed.addprocs` with some specific options.

Ensures that all workers processes use the same Julia project environment as
the current process. Requires that file systems paths are consistenst
across compute nodes.
"""
function legend_addprocs end
export legend_addprocs

function legend_addprocs(nprocs::Integer)
    Pkg.instantiate()

    # Maybe wait for shared/distributed file system to get in sync?
    # sleep(5)

    Distributed.addprocs(
        nprocs,
        exeflags = `--project=$(dirname(Pkg.project().path)) --threads=$(Base.Threads.nthreads())`
    )
    
    # Sanity check:
    worker_ids = Distributed.remotecall_fetch.(Ref(Distributed.myid), Distributed.workers())
    @assert length(worker_ids) == Distributed.nworkers()

    @info "$(Distributed.nworkers()) Julia worker processes active."
end
