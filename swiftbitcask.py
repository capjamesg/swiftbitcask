import os
import sys
import time

# this value is used to mark a deleted key
TOMBSTONE_VALUE = "DELETED"


class LockedCaskException(Exception):
    pass


def enforce_lock(func):
    def wrapper(*args, **kwargs):
        if args[0].locked:
            raise LockedCaskException("This cask is locked.")
        return func(*args, **kwargs)

    return wrapper


class SwiftCask:
    def __init__(self, directory: str):
        self.keydir = {}
        self.directory = directory
        self.open(directory=directory)
        self.locked = False
        self.write_pointer = None

    def open(self, directory: str):
        if not os.path.isdir(directory):
            os.mkdir(directory)
        datafiles = os.listdir(directory)
        datafiles.sort()
        hint_files = [file for file in datafiles if file.endswith(".hint")]
        for hint_file in hint_files:
            with open(os.path.join(directory, hint_file), "r") as f:
                for line in f.readlines():
                    key, file_id, value_pos, value_sz, tstamp = line.strip().split(",")
                    self.keydir[key] = {
                        "file_id": file_id,
                        "value_pos": int(value_pos),
                        "value_sz": int(value_sz),
                        "tstamp": tstamp,
                    }

    @enforce_lock
    def get(self, key: str):
        if key not in self.keydir:
            print("Key not found.")
            return None
        key_info = self.keydir[key]

        with open(os.path.join(key_info["file_id"]), "r") as f:
            f.seek(key_info["value_pos"])
            return f.read(key_info["value_sz"])

    @enforce_lock
    def put(self, key: str, value: str):
        if self.write_pointer is None:
            cur_time = time.time()
            self.write_pointer = open(
                os.path.join(self.directory, f"{cur_time}.data"), "w"
            )
        line = f"0,{time.time()},{len(key)},{len(value)},{key}"
        self.write_pointer.write(line + ",")
        self.write_pointer.flush()
        eol_pos = self.write_pointer.tell()
        self.write_pointer.write(value + "\n")
        self.write_pointer.flush()
        self.keydir[key] = {
            "file_id": self.write_pointer.name,
            "value_pos": eol_pos,
            "value_sz": len(value),
            "tstamp": time.time(),
            "is_tombstone": True if value == TOMBSTONE_VALUE else False,
        }

    @enforce_lock
    def delete(self, key: str):
        self.put(key, TOMBSTONE_VALUE)

    @enforce_lock
    def list_keys(self):
        return self.keydir.keys()

    @enforce_lock
    def fold(self, function, accumulator):
        for key in self.keydir.keys():
            accumulator = function(accumulator, key)
        return accumulator

    @enforce_lock
    def merge(self):
        datafiles = os.listdir(self.directory)
        datafiles.sort()
        stored_data = {}

        if not hasattr(self, "write_pointer"):
            self.write_pointer = open(f"{self.directory}/{time.time()}.data", "w")

        for file in datafiles:
            if not file.endswith(".data") or (
                hasattr(self, "write_pointer")
                and file == os.path.basename(self.write_pointer.name)
            ):
                continue

            with open(os.path.join(self.directory, file), "r") as f:
                for line in f.readlines():
                    line = line.strip()
                    crc, tstamp, key_sz, vsz, key, value = line.split(",")
                    # if tombstone, delete the key
                    stored_data[key] = {
                        "crc": int(crc),
                        "tstamp": tstamp,
                        "key_sz": sys.getsizeof(key),
                        "value_sz": len(value),
                        "key": key,
                        "value": value,
                    }

            os.remove(os.path.join(self.directory, file))

            if os.path.exists(os.path.join(self.directory, file + ".hint")):
                os.remove(os.path.join(self.directory, file + ".hint"))

        # save all data in a new file
        cur_time = time.time()
        new_file_path = os.path.join(self.directory, f"{cur_time}.data")

        with open(new_file_path, "w") as f:
            for key, data in stored_data.items():
                # print(key)
                if self.keydir[key].get("is_tombstone") == True:
                    print("Deleting key", key)
                    del self.keydir[key]
                    continue

                f.write(
                    f"0,{data['tstamp']},{data['key_sz']},{data['value_sz']},{data['key']},"
                )
                value_pos = f.tell()
                f.write(f"{data['value']}\n")

                # Update the keydir with new file information
                self.keydir[key] = {
                    "file_id": new_file_path,
                    "value_pos": value_pos,
                    "value_sz": data["value_sz"],
                    "tstamp": data["tstamp"],
                }

        # Update hint file
        new_hint_file_name = f"{new_file_path}.hint"
        with open(new_hint_file_name, "w") as f:
            for key, data in self.keydir.items():
                f.write(
                    f"{key},{data['file_id']},{data['value_pos']},{data['value_sz']},{data['tstamp']}\n"
                )

        # Close and update the write_pointer if it exists
        if hasattr(self, "write_pointer") and self.write_pointer:
            self.write_pointer.close()
            self.write_pointer = open(new_hint_file_name, "a")

    @enforce_lock
    def sync(self):
        if hasattr(self, "write_pointer"):
            self.write_pointer.flush()

    @enforce_lock
    def close(self):
        self.sync()
        self.merge()
        self.locked = True
