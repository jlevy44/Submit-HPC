import click, os
from submit_hpc.job_generator import *

CONTEXT_SETTINGS = dict(help_option_names=['-h','--help'], max_content_width=90)

@click.group(context_settings= CONTEXT_SETTINGS)
@click.version_option(version='0.1')
def job():
    pass

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
def run_torque_job(command, use_gpu, additions, queue, time, ngpu, additional_options,self_gpu_avail,imports,monitor_job,user,sleep):
    """Run torque job."""
    if isinstance(additions,tuple):
        additions=list(additions)
    elif isinstance(additions,str):
        additions=[additions]
    replace_dict = assemble_replace_dict(command, use_gpu, additions, queue, time, ngpu, self_gpu_avail, imports)
    run_torque_job_(replace_dict, additional_options, monitor_job, user, sleep)

if __name__ == '__main__':
    job()
