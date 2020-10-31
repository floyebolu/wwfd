import os
import json
import datetime
import pandas as pd


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

