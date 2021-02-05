import os, pandas as pd
import time
def qstat(user=''):
    try:
        assert len(user)>0, "Must Specify Username"
        qstat_output=list(filter(None,os.popen(f"qstat -u {user}").read().splitlines()))
        headers=qstat_output[2].strip().split()
        headers[0]=' '.join(headers[:2])
        headers.remove("ID")
        jobs=list(map(lambda x:x.strip().split(),qstat_output[4:]))
        jobs=pd.DataFrame(jobs,columns=headers)
        jobs['job_id']=jobs['Job ID'].map(lambda x: x.split(".")[0])
        jobs=jobs.set_index("job_id")
    except:
        jobs=None
    return jobs

def monitor_job_completion(job_id,user,timeout=1000,sleep=0,verbose=False):
    start=time.time()
    job_running=False
    job_status="N"
    while not job_running:
        time_elapsed=time.time()-start
        if time_elapsed>timeout: break
        if sleep: time.sleep(sleep)
        jobs=qstat(user)
        if isinstance(jobs,pd.DataFrame) and job_id in jobs.index:
            job_status=jobs.loc[job_id,"S"]
            job_running=(job_status=="R")
        if job_status=="C": break
        if verbose: print(f"Job {job_id} Status: {job_status}, Time Elapsed: [{round(time_elapsed,2)}/{timeout}]",flush=True)

    while job_running:
        time_elapsed=time.time()-start
        if time_elapsed>timeout: break
        if sleep: time.sleep(sleep)
        jobs=qstat(user)
        job_running = not any([jobs is None,job_id not in jobs.index,jobs.loc[job_id,"S"]!="R"])
        if isinstance(jobs,pd.DataFrame) and job_id in jobs.index:
            job_status=jobs.loc[job_id,"S"]
        if verbose: print(f"Job {job_id} Status: {job_status}, Time Elapsed: [{round(time_elapsed,2)}/{timeout}]",flush=True)

    return job_id,job_status
