import click, os
from submit_hpc.job_generator import *

CONTEXT_SETTINGS = dict(help_option_names=['-h','--help'], max_content_width=90)

@click.group(context_settings= CONTEXT_SETTINGS)
@click.version_option(version='0.1')
def job():
    pass

def run_torque_job_(command, use_gpu, additions, queue, time, ngpu, additional_options,self_gpu_avail,imports,monitor_job,user,sleep,verbose,work_dir="$PBS_O_WORKDIR"):
    if isinstance(additions,tuple):
        additions=list(additions)
    elif isinstance(additions,str):
        additions=[additions]
    replace_dict = assemble_replace_dict(command, use_gpu, additions, queue, time, ngpu, self_gpu_avail, imports, work_dir)
    submit_torque_job(replace_dict, additional_options, monitor_job, user, sleep, verbose)

def run_slurm_job_(command, name, ngpus, nodes, ppn, additions, time, additional_options, account, partition, gpu_share_mode, imports, work_dir):
    job_dict=dict(command=command,
                  name=name,
                  ngpus=ngpus,
                  nodes=nodes,
                  ppn=ppn,
                  additions='\n'.join(additions),
                  time=time,
                  additional_options=additional_options,
                  account=account,
                  partition=partition,
                  gpu_share_mode=gpu_share_mode,
                  imports='\n'.join(imports),
                  work_dir=work_dir)
    assemble_submit_slurm(job_dict)

@job.command()
@click.option('-c', '--command', default='', help='Command to execute through torque.', show_default=True)
@click.option('-gpu', '--use_gpu', is_flag=True, help='Specify whether to use GPUs.', show_default=True)
@click.option('-a', '--additions', multiple=True, default=[''], help='Additional commands to add.', show_default=True)
@click.option('-q', '--queue', default='default', help='Queue for torque submission, gpuq also a good one if using GPUs.', show_default=True)
@click.option('-t', '--time', default=1, help='Walltime in hours for job.', show_default=True)
@click.option('-n', '--ngpu', default=0, help='Number of gpus to request.', show_default=True)
@click.option('-ao', '--additional_options', default='', help='Additional options to add for torque run.', type=click.Path(exists=False))
@click.option('-sg', '--self_gpu_avail', is_flag=True, help='Replaces GPU search command for gpu and enforces while loop. Requires installation and import of nvgpu.', show_default=True)
@click.option('-i', '--imports', multiple=True, default=[''], help='Place above gpu assignment to make additional import statements.', show_default=True)
@click.option('-m', '--monitor_job', is_flag=True, help='Monitor job?', show_default=True)
@click.option('-u', '--user', default='', help='Username for job monitoring.', show_default=True)
@click.option('-s', '--sleep', default=3, help='Update time for job monitoring.', show_default=True)
@click.option('-v', '--verbose', is_flag=True, help='Verbose output job monitoring?', show_default=True)
@click.option('-w', '--work_dir', default='$PBS_O_WORKDIR', help='Working directory.', show_default=True)
def run_torque_job(command, use_gpu, additions, queue, time, ngpu, additional_options,self_gpu_avail,imports,monitor_job,user,sleep,verbose,work_dir):
    """Run torque job."""
    run_torque_job_(command, use_gpu, additions, queue, time, ngpu, additional_options,self_gpu_avail,imports,monitor_job,user,sleep,verbose,work_dir)

@job.command()
@click.option('-c', '--command', default='', help='Command to execute through torque.', show_default=True)
@click.option('-nm', '--name', default='', help='Job name.', show_default=True)
@click.option('-ng', '--ngpus', default=0, help='Number of gpus to request.', show_default=True)
@click.option('-n', '--nodes', default=1, help='Number of nodes to request.', show_default=True)
@click.option('-ppn', '--ppn', default=1, help='Number of processors per node to request.', show_default=True)
@click.option('-a', '--additions', multiple=True, default=[''], help='Additional commands to add.', show_default=True)
@click.option('-t', '--time', default=1, help='Walltime in hours for job.', show_default=True)
@click.option('-ao', '--additional_options', default='', help='Additional options to add for torque run.', type=click.Path(exists=False))
@click.option('-acc', '--account', default='', help='Account name.', type=click.Path(exists=False))
@click.option('-p', '--partition', default='', help='Partition of nodes.', type=click.Path(exists=False))
@click.option('-gsm', '--gpu_share_mode', default='exclusive', help='GPU share mode, nvidia-smi.', type=click.Choice(['exclusive','shared']))
@click.option('-i', '--imports', multiple=True, default=[''], help='Place above gpu assignment to make additional import statements.', show_default=True)
@click.option('-w', '--work_dir', default='', help='Working directory.', show_default=True)
def run_slurm_job(command, name, ngpus, nodes, ppn, additions, time, additional_options, account, partition, gpu_share_mode, imports, work_dir):
    """Run torque job."""
    run_slurm_job_(command, name, ngpus, nodes, ppn, additions, time, additional_options, account, partition, gpu_share_mode, imports, work_dir)


if __name__ == '__main__':
    job()
