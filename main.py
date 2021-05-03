

from dataprocessing.remoteclient import RemoteClient
from dataprocessing.restructure import Restructure
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser('Restructure Avro Data exported from Radar-base platform. Requires Python 3.7+.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--mode', help='The mode to run it in. Supports local or remote.', default='remote')
    parser.add_argument('--host', help='The SCP/SFTP server Hostname', default='')
    parser.add_argument('--user', help='The SCP/SFTP server username', default='')
    parser.add_argument('--ssh-key-filepath',
                        help='The SSH private key path for login to the SCP/SFTP server.',
                        default='/Users/yatharth/Documents/ssh/truenas')
    parser.add_argument('--source-path',
                        help='The base path in which to look for data. This is usually the parent path of the topics.',
                        default='/mnt/pool0/covid_collab/topics')
    parser.add_argument('--local-extract-path',
                        help='The local path for extraction of data.',
                        default='/Volumes/data/data/avro/restructured/')
    parser.add_argument('--num-threads',
                        help='The number of threads to use for processing files.',
                        type=int,
                        default=10)

    args = parser.parse_args()

    host = args.host
    user = args.user
    ssh_key_filepath = args.ssh_key_filepath
    remote_path = args.source_path
    num_threads = args.num_threads
    local_path = args.local_extract_path
    print(f"Starting the app with following arguments: {args}")

    # Empty value mean exclude all partitions
    exculde_data = {
        'connect_fitbit_time_zone': ['partition=1', 'partition=17']
    }

    # Empty value mean include all partitions
    include_data = {
        'connect_fitbit_intraday_heart_rate': ['partition=5', 'partition=6']
    }

    if args.mode == 'local':
        pass
    elif args.mode == 'remote':
        client = RemoteClient(host, user, ssh_key_filepath)
        for topic in client.list_dir(remote_path):
            paritions_path = remote_path + '/' + topic
            if len(include_data.keys()) != 0 and topic not in include_data.keys():
                print(f"Excluding {topic}")
                continue
            if len(exculde_data.keys()) != 0 and topic in exculde_data.keys():
                if len(exculde_data[topic]) == 0:
                    print(f"Excluding {topic}")
                    continue
            for partition in client.list_dir(paritions_path):
                if topic in exculde_data.keys():
                    if len(exculde_data[topic]) > 0 and partition in exculde_data[topic]:
                        print(f"Excluding {topic}: {partition}")
                        continue
                if topic in include_data.keys():
                    if len(include_data[topic]) > 0 and partition not in include_data[topic]:
                        print(f"Excluding {topic}: {partition}")
                        continue
                files_path = remote_path + '/' + topic + '/' + partition
                print(f"Start processing of files in {files_path}")
                restructure = Restructure(host,
                                          user,
                                          ssh_key_filepath,
                                          files_path,
                                          topic,
                                          data_extract_path=local_path)

                restructure.restructure(num_threads=num_threads)
                print(f"Processed topic {topic} and partition {partition}")
        client.disconnect()
