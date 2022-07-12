# encoding=utf8

import glob
import subprocess
import commands
from os import system
import os
import time
import requests

start_log="/opt/flink-1.13.6/job/mysql2doris/start.log"
def subprocess_popen(statement):
    global result
    process = subprocess.Popen(statement, shell=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)  # 执行shell语句并定义输出格式
    time.sleep(20)

    process.kill()

def get_job_id():
    with open(start_log, "r") as f:
        lines = f.readlines()
        job_id = lines[-1].strip()
        return job_id


def get_job_chk(job_id):
    job_checkpoint_path = "/data/flink-checkpoints/{}".format(job_id)
    res = requests.get("http://doris-be01:8081/jobs/{}/checkpoints".format(job_id))
    path = None
    # print(res.json())
    # print(res.json().get("latest"))
    if res.json().get("latest") is not  None:
        chk_id = res.json().get("latest").get("completed").get("id")
        path = '''{}/chk-{}'''.format(job_checkpoint_path, chk_id)

    else:
        for path_ in os.listdir(job_checkpoint_path):
            if path_.startswith("chk"):
                path = os.path.join(job_checkpoint_path, path_)

    return path


def get_job_id_from_overview():
    res = requests.get("http://doris-be01:8081/jobs/overview")
    jobs = res.json().get("jobs")
    job_id = None
    for job in jobs:
        if job['name'] == 'Flink CDC Mysql To Doris With Initial' and job['state'] == 'RUNNING':
            job_id = str(job['jid'])
    return job_id


def cancel_job(job_id):
    cancel_url = "http://doris-be01:8081/jobs/{}".format(job_id)
    res = requests.patch(cancel_url)
    if res.json().get("errors") is None:
        print("Job 取消成功")


def restart_job(chk):
    base_path = "/opt/flink-1.13.6/job/mysql2doris/"
    flink_path = "/opt/flink-1.13.6/bin/flink"
    jar_path = base_path + "flink_cdc.jar"
    config_path = base_path + "config.properties"
    print("开始从路径 " + chk + "重启任务")
    start_shell = "source /etc/profile && {} run -s {} --allowNonRestoredState -c com.zbkj.mysql2doris.FlinkCDCMysql2Doris {} -local_path {} ".format(
        flink_path, chk, jar_path,config_path)
    print(start_shell)
    try:
        out=os.popen(start_shell)
        print(out.readlines())
        # out= commands.getoutput(start_shell)
        # print(out)
        # command.Command.run(start_shell)
    except Exception as e:
        print(e)
        pass
    time.sleep(30)
    print(time.time())
    print("开始获取 JodID.....")
    job_id = get_job_id_from_overview()

    if job_id is not None:
        print("获取到JodID:" + job_id)
        print("开始记录JodID")
        with open(start_log, "a+") as f:
            f.write(job_id + "\n")
        print("记录完毕")
    else:
        print("未获取到JobID,重启出错")


if __name__ == '__main__':
    job_id = get_job_id_from_overview()
    checkpoint_path = "/data/flink-checkpoints"
    if job_id is not None:
        print("任务ID: " + job_id + " 运行中,开始cancel")
        cancel_job(job_id)
        chk = get_job_chk(job_id)
    else:
        print("无 Flink CDC Mysql To Doris With Initial 相关任务运行")
        print("从 start.log 中获取上次有效任务 checkpoint")
        job_id = get_job_id()

        chk = get_job_chk(job_id)
    print("JobID: "+job_id)
    print("checkpoints: "+chk)
    restart_job(chk)
