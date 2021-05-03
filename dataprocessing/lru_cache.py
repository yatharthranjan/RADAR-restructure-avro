from collections import OrderedDict
from pathlib import Path

from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from filelock import FileLock


class DataWriterLRUCache:
    # initialising capacity
    def __init__(self, capacity: int = 50):
        self.cache = OrderedDict()
        self.capacity = capacity

    # we return the value of the key
    # that is queried in O(1) and return -1 if we
    # don't find the key in out dict / cache.
    # And also move the key to the end
    # to show that it was recently used.
    def get(self, key: Path, schema) -> DataFileWriter:
        if key not in self.cache:
            return self._load_file(key, schema)
        else:
            self.cache.move_to_end(key)
            return self.cache[key]['datum_writer']

    # # first, we add / update the key by conventional methods.
    # # And also move the key to the end to show that it was recently used.
    # # But here we will also check whether the length of our
    # # ordered dictionary has exceeded our capacity,
    # # If so we remove the first key (least recently used)
    # def put(self, key: Path, value: dict) -> None:
    #     self.cache[key] = value
    #     self.cache.move_to_end(key)
    #     if len(self.cache) > self.capacity:
    #         self._remove_item()

    def _load_file(self, file_path, schema) -> DataFileWriter:
        f = open(file_path, 'ab+')
        self.cache[file_path] = dict()
        self.cache[file_path]['file_io'] = f
        writer = DataFileWriter(f, DatumWriter(), schema)
        self.cache[file_path]['datum_writer'] = writer
        self.cache.move_to_end(file_path)
        if len(self.cache) > self.capacity:
            self._remove_item()
        return writer

    # Remove the least used item and close all handles and locks
    def _remove_item(self):
        _, v = self.cache.popitem(last=False)
        v['datum_writer'].close()
        v['file_io'].close()

    def close(self):
        for _, v in self.cache.items():
            v['datum_writer'].close()
            v['file_io'].close()
