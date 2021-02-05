"""
job_generator.py
=======================
Wraps and runs your commands through torque.
"""

import os
from submit_hpc.job_monitor import monitor_job_completion

def assemble_replace_dict(command, use_gpu, additions, queue, time, ngpu, self_gpu_avail, imports):
    """Create dictionary to update BASH submission script for torque.

    Parameters
    ----------
    command : type
        Command to executer through torque.
    use_gpu : type
        GPUs needed?
    additions : type
        Additional commands to add (eg. module loads).
    queue : type
        Queue to place job in.
    time : type
        How many hours to run job for.
    ngpu : type
        Number of GPU to use.

    Returns
    -------
    Dict
        Dictionary used to update Torque Script.

    """
    if isinstance(additions,(list,tuple)):
        additions='\n'.join(additions)
    if isinstance(imports,(list,tuple)):
        imports='\n'.join(imports)

    replace_dict = {'COMMAND':command,
                'IMPORTS':imports,
                'GPU_SETUP':("""gpuNum=`cat $PBS_GPUFILE | sed -e 's/.*-gpu//g'`
unset CUDA_VISIBLE_DEVICES
export CUDA_VISIBLE_DEVICES=$gpuNum""" if use_gpu else '') if not self_gpu_avail else """export gpuNum=$(nvgpu available | tr ',' '\\n' | shuf | head -n 1); while [ -z $(echo $gpuNum) ]; do export gpuNum=$(nvgpu available | tr ',' '\\n' | shuf | head -n 1); done""",
                'NGPU':f'#PBS -l gpus={ngpu}' if (use_gpu and ngpu) else '',
                'USE_GPU':"#PBS -l feature=gpu" if (use_gpu and ngpu) else '',
                'TIME':str(time),'QUEUE':queue,'ADDITIONS':additions}
    return replace_dict

def submit_torque_job(replace_dict, additional_options="", monitor_job=False, user='', sleep=3, verbose=False):
    """Run torque job after creating submission script.

    Parameters
    ----------
    replace_dict : type
        Dictionary used to replace information in bash script to run torque job.
    additional_options : type
        Additional options to pass scheduler.

    Returns
    -------
    str
        Custom torque job name.

    """
    txt="""#!/bin/bash -l
#PBS -N run_torque
#PBS -q QUEUE
NGPU
USE_GPU
#PBS -l walltime=TIME:00:00
#PBS -j oe
cd $PBS_O_WORKDIR
IMPORTS
GPU_SETUP
ADDITIONS
COMMAND"""
    for k,v in replace_dict.items():
        txt = txt.replace(k,v)
    with open('torque_job.sh','w') as f:
        f.write(txt)
    job=os.popen(f"mksub torque_job.sh {additional_options}").read().strip('\n')
    job_id=job.split(".")[0]
    completion_status=None
    print(f"Submitted job: {job}")
    if monitor_job:
        print(f"Monitoring job: {job}")
        job_id, completion_status=monitor_job_completion(job_id,user,timeout=int(replace_dict['TIME'])*3600,sleep=sleep,verbose=verbose)
    return job, job_id, completion_status

def assemble_run_torque(command, use_gpu, additions, queue, time, ngpu, additional_options="",):
    """Runs torque job after passing commands to setup bash file.

    Parameters
    ----------
    command : type
        Command to executer through torque.
    use_gpu : type
        GPUs needed?
    additions : type
        Additional commands to add (eg. module loads).
    queue : type
        Queue to place job in.
    time : type
        How many hours to run job for.
    ngpu : type
        Number of GPU to use.
    additional_options : type
        Additional options to pass to Torque scheduler.

    Returns
    -------
    job
        Custom job name.

    """
    job = run_torque_job_(assemble_replace_dict(command, use_gpu, additions, queue, time, ngpu),additional_options, self_gpu_avail, imports)
    return job
