"""
job_generator.py
=======================
Wraps and runs your commands through torque.
"""

import os
from submit_hpc.job_monitor import monitor_job_completion

def assemble_replace_dict(command, use_gpu, additions, queue, time, ngpu, self_gpu_avail, imports, work_dir):
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
                'WORKDIR':work_dir,
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
cd WORKDIR
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

def assemble_submit_slurm(job_dict):
    gpu_txt=f"#SBATCH --gres=gpu:{job_dict.get('ngpus',0)}" if job_dict.get("ngpus",0) else "" # --gpus=
    account_txt=f"#SBATCH --account={job_dict.get('account','')}" if job_dict.get("account","") else ""
    partition_txt=f"#SBATCH --partition={job_dict.get('partition','')}" if job_dict.get("partition","") else ""
    gpu_sharing_mode_txt=f"#SBATCH --gpu_cmode={job_dict.get('gpu_share_mode','exclusive')}" if (job_dict.get('gpu_share_mode','exclusive')!='exclusive' and job_dict.get("ngpus",0)) else ''
    nodes_txt=f"#SBATCH --nodes={job_dict.get('nodes',1)}" if job_dict.get("nodes",1) else ""
    deprecated_options=f"""#SBATCH --cpus-per-gpu={job_dict.get("cpu_gpu",8)}
#SBATCH --cpus-per-task=1
"""
    directives=f"""#!/bin/bash
#SBATCH --chdir={job_dict.get("work_dir",os.getcwd()) if job_dict.get("work_dir","") else os.getcwd()}
{nodes_txt}
#SBATCH --ntasks-per-node={job_dict.get("ppn",1)}
#SBATCH --time={job_dict.get("time",1)}:00:00
#SBATCH --job-name={(job_dict.get("name","slurm_job") if job_dict.get("name","") else "")}
#SBATCH --mem={job_dict.get("mem",8)}G
{gpu_txt}
{account_txt}
{partition_txt}
{gpu_sharing_mode_txt}
{"" if job_dict.get("no_bashrc",False) else "source ~/.bashrc"}
cd {job_dict.get("work_dir",os.getcwd()) if job_dict.get("work_dir","") else os.getcwd()}
{job_dict.get("imports","")}
{job_dict.get("additions","")}
{job_dict.get("command","")}
    """
    with open("slurm_job.sh",'w') as f:
        f.write(directives)
    job=os.popen(f"sbatch slurm_job.sh {job_dict.get('additional_options','')}").read().strip('\n')
    print(job)
    return job
