import os
import json
import datetime
import pandas as pd
import shutil


def read_json(folder_path, file_name='message_', file_extension='json', msg_index=[1]):
    root_path = os.path.realpath('../../.data/')
    folder_path = os.path.join(root_path, folder_path)
    for i in msg_index:
        msg = os.path.join(folder_path, f'{file_name}{i}.{file_extension}')
        # print(msg, ': ', os.path.isfile(msg))
        with open(msg, 'r', encoding='utf-8') as f:
            jmsg = json.load(f)
        yield jmsg


def convert_ms_to_date(ms):
    date_obj = datetime.datetime.fromtimestamp(ms/1000)
    return date_obj.date()


def index_msgs(msgs):
    index = []
    message_list = []
    for msg in msgs:
        message_list += msg['messages']
    for i, message in enumerate(message_list):
        date = convert_ms_to_date(message['timestamp_ms'])
        if len(index) == 0:
            index.append([date, i])
            continue
        if index[-1][0] != date:
            index.append([date, i])
    index_df = pd.DataFrame(index, columns=['Date', 'MsgIndex'])
    return index_df


def copy_msgs(src, dst):
    src = os.path.realpath(src)
    dst = os.path.realpath(dst)
    os.makedirs(dst, exist_ok=True)
    orig = []
    dest = []
    for pf in os.scandir(src):
        if not pf.is_dir() or pf.name.startswith('facebookuser'):
            continue
        for mfile in os.scandir(pf.path):
            if not mfile.is_dir() and mfile.name.startswith('message'):
                orig.append(mfile.path)
                dest.append(os.path.join(dst, pf.name, mfile.name))

    for (f1, f2) in zip(orig, dest):
        os.makedirs(os.path.dirname(f2), exist_ok=True)
        shutil.copy2(f1, f2)
