# This file is a part of LegendDataManagement.jl, licensed under the MIT License (MIT).


"""
    LegendDataManagement.legend_distributed_imports(procs::AbstractVector{<:Integer})

Import required packages for internal operations on processes `procs`.
"""
function legend_distributed_imports(@nospecialize(procs::AbstractVector{<:Integer}))
    Distributed.remotecall_eval(Main, procs,
        quote
            import Distributed, ThreadPinning, LinearAlgebra, LegendDataManagement
        end
    )
end


"""
    pinthreads_default()

Use default thread-pinning strategy.
"""
function legend_pinthreads()
    if Distributed.myid() == 1
        let n_juliathreads = Base.Threads.nthreads()
            if n_juliathreads > 1
                LinearAlgebra.BLAS.set_num_threads(n_juliathreads)
            end
        end
    else
        let available_cpus = ThreadPinning.affinitymask2cpuids(ThreadPinning.get_affinity_mask())
            ThreadPinning.pinthreads(:affinitymask)
            LinearAlgebra.BLAS.set_num_threads(length(available_cpus))
        end
    end
end
export legend_pinthreads


"""
    LegendDataManagement.legend_distributed_pinthreads(procs::AbstractVector{<:Integer})

Use default thread-pinning strategy on processes `procs`.
"""
function legend_distributed_pinthreads(@nospecialize(procs::AbstractVector{<:Integer}))
    if 1 in procs
        legend_pinthreads()
    end

    workerprocs = filter(!isequal(1), procs)
    if !isempty(workerprocs)
        Distributed.remotecall_eval(Main, workerprocs,
            quote
                import LegendDataManagement
                LegendDataManagement.legend_pinthreads()
            end
        )
    end
end


"""
    LegendDataManagement.shutdown_workers_atexit()

Ensure worker processes are shut down when Julia exits.
"""
function shutdown_workers_atexit()
    atexit(() -> Distributed.rmprocs(filter!(!isequal(1), Distributed.workers()), waitfor = 1))
end


"""
    LegendDataManagement.distributed_resources

Get the distributed Julia process resources currently available.
"""
function distributed_resources()
    resources = Distributed.remotecall_fetch.(
        () -> (
            workerid = Distributed.myid(),
            hostname = Base.gethostname(),
            nthreads = Base.Threads.nthreads(),
            blas_nthreads = LinearAlgebra.BLAS.get_num_threads(),
            cpuids = ThreadPinning.getcpuids()
        ),
        Distributed.workers()
    )
    StructArray(resources)
end


"""
    legend_addprocs()
    legend_addprocs(nprocs::Integer)

Add Julia worker processes for LEGEND data processing.

Automatically chooses between

* Adding processes on the current host
* Adding processes via SLURM (when in an `salloc` or `sbatch`) environment

Ensures that all workers processes use the same Julia project environment as
the current process. Requires that file systems paths are consistenst
across compute hosts.

See also [`LegendDataManagement.distributed_resources()`](@ref) and
[`LegendDataManagement.shutdown_workers_atexit()`](@ref).
"""
function legend_addprocs end
export legend_addprocs

legend_addprocs(; kwargs...) = _default_addprocs()(; kwargs...)
legend_addprocs(@nospecialize(nprocs::Integer); kwargs...) = _default_addprocs()(Int(nprocs); kwargs...)

function _default_addprocs()
    if haskey(ENV, "SLURM_JOB_ID") && !haskey(ENV, "SLURM_STEP_ID")
        # In salloc- or sbatch-spawned environment, but not within srun:
        return _addprocs_slurm
    else
        return _addprocs_localhost
    end
end


_addprocs_localhost(; kwargs...) = _addprocs_localhost(1; kwargs...)

function _addprocs_localhost(nprocs::Int)
    @info "Adding $nprocs Julia processes on current host"

    # Maybe wait for shared/distributed file system to get in sync?
    # sleep(5)

    julia_project = dirname(Pkg.project().path)
    worker_nthreads = Base.Threads.nthreads()

    new_workers = Distributed.addprocs(
        nprocs,
        exeflags = `--project=$julia_project --threads=$worker_nthreads`
    )

    @info "Configuring $nprocs new Julia worker processes"

    legend_distributed_imports(new_workers)

    # Sanity check:
    worker_ids = Distributed.remotecall_fetch.(Ref(Distributed.myid), Distributed.workers())
    @assert length(worker_ids) == Distributed.nworkers()

    @info "Added $(length(new_workers)) Julia worker processes on current host"
end


function _addprocs_slurm(; kwargs...)
    slurm_ntasks = parse(Int, ENV["SLURM_NTASKS"])
    slurm_ntasks > 1 || throw(ErrorException("Invalid nprocs=$slurm_ntasks inferred from SLURM environment"))
    _addprocs_slurm(slurm_ntasks; kwargs...)
end

function _addprocs_slurm(
    nprocs::Int;
    job_file_loc::AbstractString = joinpath(homedir(), "slurm-julia-output"),
    retry_delays::AbstractVector{<:Real} = [1, 1, 2, 2, 4, 5, 5, 10, 10, 10, 10, 20, 20, 20]
)
    @info "Adding $nprocs Julia processes via SLURM"

    julia_project = dirname(Pkg.project().path)
    slurm_ntasks = nprocs
    slurm_nthreads = parse(Int, ENV["SLURM_CPUS_PER_TASK"])
    slurm_mem_per_cpu = parse(Int, ENV["SLURM_MEM_PER_CPU"]) * 1024^2
    slurm_mem_per_task = slurm_nthreads * slurm_mem_per_cpu

    cluster_manager = ClusterManagers.SlurmManager(slurm_ntasks, retry_delays)
    worker_timeout = round(Int, max(sum(cluster_manager.retry_delays), 60))
    ENV["JULIA_WORKER_TIMEOUT"] = "$worker_timeout"
    
    mkpath(job_file_loc)
    new_workers = Distributed.addprocs(
        cluster_manager, job_file_loc = job_file_loc,
        exeflags = `--project=$julia_project --threads=$slurm_nthreads --heap-size-hint=$(slurm_mem_per_task÷2)`,
        cpus_per_task = "$slurm_nthreads", mem_per_cpu="$(slurm_mem_per_cpu >> 30)G", # time="0:10:00",
        mem_bind = "local", cpu_bind="cores",
    )

    @info "Configuring $nprocs new Julia worker processes"

    legend_distributed_imports(new_workers)
    legend_distributed_pinthreads(new_workers)

    @info "Added $(length(new_workers)) Julia worker processes via SLURM"
end
