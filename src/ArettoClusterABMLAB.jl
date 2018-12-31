__precompile__(true)
## Julia Parallel Computing documentation: https://docs.julialang.org/en/v1/manual/parallel-computing/index.html 
module ArettoClusterABMLAB
    using Distributed
    import Distributed.launch, Distributed.manage, Distributed.kill, Distributed.init_worker, Distributed.connect
    export ArettoManager
    export launch, manage
    
    #worker_arg = `--worker`

    #function __init__()
    #    global worker_arg
    #    worker_arg = `--worker=$(Distributed.cluster_cookie())`
    #end
    

    worker_arg() = `--worker=$(Distributed.init_multi(); cluster_cookie())`


    struct ArettoManager <: ClusterManager
        np::Integer
    end

    function launch(manager::ArettoManager, params::Dict, launched::Array, c::Condition)
        try
            # cleanup old files
            map(rm, filter(t -> occursin(r"job.*\.out", t), readdir(exehome)))

            ## get the default arguments passed to addprocs()  
            ## filter the non-default keys from stdkeys
            ## the filtered keys are the ones that are sent to Slurm
            ## TODO if jobname supplied in params, don't supply default name.
            ## TODO workerarg can be passed in exeflags
            stdkeys  = keys(Distributed.default_addprocs_params()) 
            p = filter(x->!(x[1] in stdkeys), params)
            srunargs = []
            for k in keys(p)
                if length(string(k)) == 1    
                    push!(srunargs, "-$k")    
                    val = p[k]    
                    if length(val) > 0    
                        push!(srunargs, "$(p[k])")    
                    end    
                else    
                    k2 = replace(string(k), "_"=>"-")    
                    val = p[k]    
                    if length(val) > 0    
                        push!(srunargs, "--$(k2)=$(p[k])")    
                    else    
                        push!(srunargs, "--$(k2)")    
                    end    
                end    
            end
   
            exehome  = params[:dir]
            exename  = params[:exename]
            exeflags = params[:exeflags] 
            
            
            np = manager.np
            jobname = "julia-$(getpid())" ## use default jobname.. 
      
            srun_cmd = `srun -J $jobname -n $np -o "job%4t.out" -D $exehome $(srunargs) $exename $exeflags $(worker_arg())`
            srun_proc = open(srun_cmd)
           
            @debug "passed in params: $params"
            @debug "default keys: $stdkeys"
            @debug "filtered keys is: $p"
            @debug "constructed srun args: $srunargs"
            @debug "$srun_cmd"    
            @debug "srun_process: $(dump(srun_proc))"

            ## the `srun` Slurm command is now running. 
            ## When the job allocates, it will create number $np text files called "job$4t.out" which have the ip address of the allocated resource where the worker Julia has laucnhed
            ## if a Julia worker is successfully launched, it writes julia_worker:9040#172.16.1.27 to STDOUT
            ## the STDOUT is rerouted to the jobfile by srun which is why -J is important
            for i = 0:(np - 1)
                print("connecting to worker $(i + 1) out of $np\r")
                local w=[]
                fn = "$exehome/job$(lpad(i, 4, "0")).out"
                t0 = time()
                ## wait for a bit (atleast a min).
                while true
                    if time() > t0 + 60 + np
                        warn("dropping worker: file not created in $(60 + np) seconds")
                        break
                    end
                    sleep(0.01)
                    ## if a valid output file is made by Slurm, open the file and read the ip address/hostname
                    if isfile(fn) && filesize(fn) > 0
                        w = open(fn) do f
                            return split(split(readline(f), ":")[2], "#")
                        end
                        break
                    end
                end
                ## if we get a valid hostname/ipaddress add a WorkerConfig object
                if length(w) > 0
                    config = WorkerConfig()
                    config.port = parse(Int, w[1])
                    config.host = strip(w[2])
                    # Keep a reference to the proc, so it's properly closed once
                    # the last worker exits.
                    config.userdata = srun_proc
                    push!(launched, config)
                    notify(c)
                end
            end
        catch e
            println("Error launching Slurm job.")
            rethrow(e)
        end
    end

    function manage(manager::ArettoManager, id::Integer, config::WorkerConfig, op::Symbol)
        # This function needs to exist, but so far we don't do anything
        # manage(manager::FooManager, id::Integer, config::WorkerConfig, op::Symbol) is called at different times during the worker's lifetime with appropriate op values:
        # with :register/:deregister when a worker is added / removed from the Julia worker pool.
        # with :interrupt when interrupt(workers) is called. The ClusterManager should signal the appropriate worker with an interrupt signal.
        # with :finalize for cleanup purposes.
    end
    addprocs_slurm(np::Integer; kwargs...) = addprocs(ArettoManager(np); kwargs...)
end

#using .Aretto
#addprocs(a, partition="defq", N=16)
#include("C:\\Users\\affan\\Documents\\abm-lab-cluster\\Aretto.jl")
