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
    date_obj = datetime.datetime.fromtimestamp(ms / 1000)
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


def create_indices_for_all(data_path='../../.data'):
    # Get all person/message folders.
    data_path = os.path.realpath(data_path)
    # Then for each:
    for pf in os.scandir(data_path):
        if not pf.is_dir():
            continue
        n = len([mf for mf in os.scandir(pf.path) if mf.name.startswith('message')])
        # Use read_json to read in the message files
        jmsg = read_json(pf.name, 'message_', 'json', list(range(1, n+1)))
        # Use index_msgs to index the messages by date
        index = index_msgs(jmsg)
        # Use return from index_msgs and save to csv file in person/message folder
        index.to_csv(os.path.join(pf.path, 'date_index.dat'), sep='\t')


def create_master_index(data_path='../../.data', myself='Folarin Oyebolu'):
    # get all person/message folders
    data_path = os.path.realpath(data_path)
    start_date = datetime.date(2008, 1, 1)
    end_date = datetime.date(2037, 12, 31)
    d = pd.date_range(start_date, end_date, freq='D')
    big_index = pd.DataFrame([[False, []]] * len(d), index=d, columns=['interaction', 'contacts'])
    bic = big_index
    contacts_col = []
    interaction_col = []
    for i, pf in enumerate(os.scandir(data_path)):
        if not pf.is_dir():
            continue
        # Extract participants from message_1.json and remove myself from list
        jmsg = next(read_json(pf.name))
        all_participants = jmsg['participants']
        contacts = [c['name'] for c in all_participants if c['name'] != myself]
        # read in index df
        index = pd.read_table(os.path.join(pf.path, 'date_index.dat'), index_col=0)
        # set dates as row labels, create interaction and contact columns
        index['Date'] = pd.to_datetime(index['Date'])
        index = index.set_index('Date')
        index['interaction'] = True
        index['contacts'] = [[contacts] * index.shape[0]][0]
        # join index df to bic df
        bic = bic.join(index, rsuffix=str(i))
        contacts_col.append(f'contacts{i}')
        interaction_col.append(f'interaction{i}')
    # tidy up the master index df
    interactions = bic.loc[:, interaction_col].any(axis='columns')
    bic['interaction'] = interactions
    bic = bic.rename(columns={'MsgIndex': 'MsgIndex0'})
    bic = bic.drop(columns=['contacts'])
    return bic
