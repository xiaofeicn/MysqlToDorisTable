# encoding=utf8

import glob
import subprocess
from os import system
import os
import time

import requests


def subprocess_popen(statement):
    result = []
    is_error = 0
    try:
        p = subprocess.Popen(statement, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)  # 执行shell语句并定义输出格式
        time.sleep(20)

        p.kill()
    #     output, unused_err = p.communicate()
    #     output = output.decode("utf-8")
    #     print(output)
    #     for i in range(len(output)):  # 由于原始结果需要转换编码，所以循环转为utf8编码并且去除\n换行
    #         res = output[i]
    #         if "Caused by:" in res:
    #             is_error = 1
    #         result.append(res)
    #     return is_error, result
    except Exception as e:
        print("-" * 20)


def get_job_id():
    with open("start.log", "r") as f:
        lines = f.readlines()

        if "JobID" in lines[-1]:
            job_id = lines[-1].split("JobID")[1].strip()
        else:
            job_id = lines[-1].strip()
        return job_id


def get_job_id_from_overview():
    res = requests.get("http://doris-be01:8081/jobs/overview")
    jobs = res.json().get("jobs")
    job_id = ""
    for job in jobs:
        if job['name'] == 'Flink CDC Mysql To Doris With Initial' and job['state'] == 'RUNNING':
            job_id = str(job['jid'])
    return job_id


def cancel_job(flink_path, job_id):
    if len(job_id) > 1:
        cancel_shell = ''' {} cancel {}'''.format(flink_path, job_id)
        system(cancel_shell)


def restart_job(checkpoint_path, base_path, job_id):
    job_checkpoint_path = os.path.join(checkpoint_path, job_id)
    jar_path = base_path + "flink_cdc.jar"
    for path in os.listdir(job_checkpoint_path):
        if path.startswith("chk"):
            checkpoint_dir = os.path.join(job_checkpoint_path, path)
            print("开始从路径 " + checkpoint_dir + "重启任务")
            start_shell = '''{} run -s {} --allowNonRestoredState -c com.zbkj.mysql2doris.flinkCDCMysql2Doris {} '''.format(
                flink_path, checkpoint_dir, jar_path)
            subprocess_popen(start_shell)
            time.sleep(20)
            print("开始获取 JodID.....")
            job_id = get_job_id_from_overview()


            if len(job_id) > 1:
                print("获取到JodID:"+job_id)
                print("开始记录JodID")
                with open("start.log", "a+") as f:
                    f.write(job_id + "\n")
                print("记录完毕")
            else:
                print("未获取到JobID,重启出错")


if __name__ == '__main__':
    job_id = get_job_id_from_overview()
    checkpoint_path = "/data/flink-checkpoints"
    base_path = "/opt/flink-1.13.6/job/mysql2doris/"
    flink_path = "/opt/flink-1.13.6/bin/flink"
    if len(job_id) > 1:
        print("任务ID: " + job_id + " 运行中,开始cancel")
        cancelled = cancel_job(flink_path, job_id)
        time.sleep(5)
    else:
        print("无 Flink CDC Mysql To Doris With Initial 相关任务运行")
        print("从 start.log 中获取上次有效任务 checkpoint")
        job_id = get_job_id()
    restart_job(checkpoint_path, base_path, job_id)
