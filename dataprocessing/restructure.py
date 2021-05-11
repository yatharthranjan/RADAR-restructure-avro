import threading

from filelock import FileLock
from paramiko.ssh_exception import SSHException

from .remoteclient import RemoteClient
import datetime
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import copy
import avro
from avro.datafile import DataFileReader
from avro.io import DatumReader
import avro.schema
import concurrent.futures
import pickle
from .lru_cache import DataWriterLRUCache
import traceback
import time as time_profile

processed_files_lock = threading.Lock()


class Restructure:

    def __init__(self, host: str, user: str, ssh_key_path: str, files_path: str, data_type: str, time_column='time',
                 data_extract_path='data/avro/', max_files_per_thread=10):
        self.files_path = files_path
        self.time_column = time_column
        self.data_extract_path = data_extract_path
        self.data_type = data_type
        self.host = host
        self.user = user
        self.ssh_key_path = ssh_key_path
        self.max_files_per_thread = max_files_per_thread
        os.makedirs(Path(os.path.join('tmp', 'processed_files')), exist_ok=True)
        os.makedirs(os.path.join('tmp', 'file_list'), exist_ok=True)
        self.processed_files_path = Path(os.path.join('tmp', 'processed_files', self.files_path.replace('/', '_')))
        self.file_list_path = Path(os.path.join('tmp', 'file_list', self.files_path.replace('/', '_')))

    def get_file_list(self):
        if self.file_list_path.exists():
            # read the list of remote files stored in the file.
            print(f'File list found in file: {self.file_list_path}')
            with open(self.file_list_path, 'rb') as f:
                file_list = pickle.load(f)
        else:
            print(f'File list not found in file: {self.file_list_path}. Downloading from server...')
            # get files from remote server and save to file.
            remote_client = RemoteClient(self.host, self.user, self.ssh_key_path)
            file_list = remote_client.list_dir(self.files_path)
            remote_client.disconnect()
            with open(self.file_list_path, 'wb+') as f:
                pickle.dump(file_list, f)
        return file_list

    def restructure(self, num_threads=10):
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            i = 0
            futures = []
            processed_files = ""

            if self.processed_files_path.exists():
                with processed_files_lock:
                    with open(self.processed_files_path, 'r') as pf:
                        processed_files = pf.read()
                        count_processed = processed_files.count("\n")
                        print(f'Number of files already processed: {count_processed}')

            for file_name in self.get_file_list():
                file_path = self.files_path + '/' + file_name
                if file_path in processed_files:
                    # print(f"Already processed file {file_path}.")
                    continue
                futures.append(executor.submit(self.process_data, file_path))

            print('Scheduled Tasks. Waiting for Futures to complete...')
            start = time_profile.time()
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    print(res)
                except Exception as e:
                    print(f"Error while processing file", e, traceback.format_exc())
                finally:
                    i += 1
                    if i % 100 == 0:
                        print(f"Processed {i} files. Remaining in this path: {len(futures) - i}."
                              f" Time taken: {time_profile.time() - start}")
                        start = time_profile.time()

    def process_data(self, file_path) -> str:
        """
        This restructures the data into the format /{projectId}/{userId}/{date}.avro
        So groups by projectId and userId and creates a single avro for each day.
        """

        print(f"Processing file {file_path}")

        file_name = file_path.split('/')[-1]
        tmp_file_path = Path(os.path.join('tmp', file_name))

        if not tmp_file_path.exists():
            try:
                remote_client = RemoteClient(self.host, self.user, self.ssh_key_path)
                remote_client.download_file(file_path, f'tmp/{file_name}')
                remote_client.disconnect()
            except SSHException as sshe:
                print(f"Could not download the file, will wait and try again: ", sshe)
                # Make this thread sleep so we don't hit SSH limits
                time_profile.sleep(0.1)
                return self.process_data(file_path)

        with open(f'tmp/{file_name}', 'rb') as f:
            try:
                reader = DataFileReader(f, DatumReader())
            except ValueError:
                print(f"File {file_name} was not downloaded properly. Trying again...")
                os.remove(f'tmp/{file_name}')
                return self.process_data(file_path)
            metadata = copy.deepcopy(reader.meta)
            schema_avro = avro.schema.parse(metadata['avro.schema'])
            datum = [data for data in reader]
            reader.close()

        data_writer_cache = DataWriterLRUCache()

        for data in datum:
            project_id = data['key']['projectId']
            user_id = data['key']['userId']
            data_path = os.path.join(self.data_extract_path, project_id, user_id, self.data_type)

            if not Path(data_path).exists():
                os.makedirs(data_path, exist_ok=True)

            try:
                time = datetime.datetime.fromtimestamp(data['value']['time'])
            except KeyError:
                time = datetime.datetime.fromtimestamp(data['value']['timeReceived'])

            my_file = Path(os.path.join(data_path, str(time.date()) + '.avro'))

            lock = FileLock(str(my_file.absolute()) + '.lock')

            with lock:
                # get writer from cache
                writer = data_writer_cache.get(my_file, schema_avro)
                writer.append(data)
                # with open(my_file, 'ab+') as f:
                #     writer = DataFileWriter(f, DatumWriter(), schema_avro)
                #     writer.append(data)
                #     writer.close()

            if Path(lock.lock_file).exists():
                # print('Fucking lock still exists. MoFo won\'t die. Kill with bazooka!')
                os.remove(lock.lock_file)

        data_writer_cache.close()

        if tmp_file_path.exists():
            os.remove(f'tmp/{file_name}')

        with processed_files_lock:
            with open(self.processed_files_path, 'a+') as pf:
                pf.write(f"{file_path}\n")

        return f"Processed file: {file_path}"
