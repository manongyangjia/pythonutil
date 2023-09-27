# -*- coding: utf-8 -*-
import time
import sys
import requests
from telnetlib import Telnet
import json


parame='kafka准生产节点:127.0.0.1:9091,9092,9093'
parames=parame.splitlines()
data_list=[]
for v in parames:
    vv=v.split(":")
    print(vv[1]+","+vv[2])
    for port in vv[2].split(","):
        print(f"开始检测服务器{vv[1]}端口{port}:")
        data=f"{vv[1]}端口{port}"
        try:
            Telnet(vv[1], port,5)  # 超时时间5s
            print("OK")
            result=0
        except:
            print("Failed")
            print("不可用")
            data=data+"不可用"
            data_list.append(data)


if data_list:
    print(data_list)
    str=repr(data_list)
       
    webhoo_url='https://open.feishu.cn/open-apis/bot/v2/hook/111111111111111111111111111'
    data={"msg_type": "text","content": {"text": "<at user_id=\"ou_xxx\">所有人</at>\n"+str}}
    resp = requests.post(url=webhoo_url,
    headers={'Content-Type': 'application/json; charset=UTF-8'},
    data=json.dumps(data)
    )
    print("发送消息成功")
   
