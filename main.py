import os
import heapq
import numpy as np
import sys
import shutil


def split_n_sort():
    directory = "temp"
    parent_dir = os.getcwd()
    path = os.path.join(parent_dir, directory)
    os.mkdir(path)

    for i in range(8):
        to_file_path = os.getcwd() + "/temp/output_" + str(i)
        with open(sys.argv[1], 'rb') as file:
            file.seek(i * 8 * (2 ** 27))
            arr = np.fromfile(file, dtype=np.uint64, count=2 ** 27)
            np.ndarray.sort(arr)
        with open(to_file_path, 'wb'):
            arr.tofile(to_file_path)


class Buffer:
    def __init__(self, file):
        self.file = file
        self.buffer = np.fromfile(file, dtype=np.uint64, count=2 ** 23)
        self.already_read = 0
        self.offset = 0
        self.max_size = 2 ** 23


def merge():
    with open(sys.argv[2], 'w+') as f:
        f.close()

    dir_path = os.getcwd() + "/temp"
    list_of_files = os.listdir(dir_path)

    heap = []
    buff_list = []
    output_buffer = []
    max_buff = 2 ** 23

    for i in range(len(list_of_files)):
        buff_list.append(Buffer(dir_path + "/" + list_of_files[i]))
        heap.append((buff_list[i].buffer[0], i))
        buff_list[i].already_read += 1

    heapq.heapify(heap)

    for i in range(128):
        for j in range(max_buff):
            val, file_ind = heapq.heappop(heap)
            output_buffer.append(val)

            buff = buff_list[file_ind]

            if buff.already_read >= buff.buffer.size:

                buff.offset += 1
                with open(buff.file, 'rb') as f:
                    f.seek(buff.offset * 8 * buff.max_size)
                    buff.buffer = np.fromfile(f, dtype=np.uint64, count=buff.max_size)

                buff.already_read = 0

                if buff.buffer.size == 0:
                    continue

            val = buff.buffer[buff.already_read]
            buff.already_read += 1

            next_tuple = (val, file_ind)
            heapq.heappush(heap, next_tuple)
            
        with open(sys.argv[2], 'ab') as to_file:
            output_buffer = np.array(output_buffer, dtype=np.uint64)
            output_buffer.tofile(to_file)
            output_buffer = []



    if len(output_buffer) > 0:
        with open(sys.argv[2], 'ab') as to_file:
            output_buffer = np.array(output_buffer, dtype=np.uint64)
            output_buffer.tofile(to_file)
            output_buffer = []


split_n_sort()

merge()

location = os.getcwd()
path = os.path.join(location, "temp")

shutil.rmtree(path)
