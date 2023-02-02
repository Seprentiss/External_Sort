import os
import heapq
import numpy as np
import sys
import shutil

""" 
Function to split and sort the data into 8 1GB files.
"""

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


""" 
Creates a Buffer object. (An Array which holds 2^23 bytes or 1048576 Integers)

Takes in a binary file as input.

Once empty the Buffer will refill with new data until it reaches the end of the file
"""          
class Buffer:
    def __init__(self, file):
        self.file = file
        self.buffer = np.fromfile(file, dtype=np.uint64, count=2 ** 23)
        
        # Keeps tack of how many bytes have been read
        self.already_read = 0
        # An offset for how far ahead to read in the file when refilling the Buffer
        self.offset = 0
        # The maximum size of the Buffer
        self.max_size = 2 ** 23


""" 
Function to merge all the the newly created temp files into 1 sorted 8GB output file

Gathers each of the 8 temp files and creates Buffers for them. Then adds new data to a Min Priority Queue that sorts all of 
the data in chuncks of 2^23 bytes.

"""
def merge():
    
    with open(sys.argv[2], 'w+') as f:
        f.close()

    dir_path = os.getcwd() + "/temp"
    list_of_files = os.listdir(dir_path)
    
    # initialize the Priority min Queue
    heap = []
    
    # list that will contain 8 Buffers, one for each temp file
    buff_list = []
    
    # will contain all of the info that will be appended to the output file
    output_buffer = []
    
    # Maximum size of the output Buffer
    max_buff = 2 ** 23
    
    
    for i in range(len(list_of_files)):
        """
        Loop through each of the 8 temp files and create Bufffers for all of them and then add them to buff_list.
        Then append a tuple for each Buffer to the heap. The tuple contains the 1st element in the Buffer and the index of 
        the Buffer in buff_list. Finally increase each Buffers already_read value by 1.
        
        """
        buff_list.append(Buffer(dir_path + "/" + list_of_files[i]))
        heap.append((buff_list[i].buffer[0], i))
        buff_list[i].already_read += 1

    # heapify the heap
    heapq.heapify(heap)
    
    
    for i in range(128):
        """
        Loop through 128 times because 2^30 bytes is equal to 1 GB and since each output buffer contains 2^23 bytes we need to run the loop
        2^30 / 2^23 times to merge all of the data.
        
        """
        
        # Loop through for max_buff iteratrions to fill the output Buffer 
        for j in range(max_buff):
            
            # Extract the value and Buffer index from the heap. This will be the lowest value every loop
            val, file_ind = heapq.heappop(heap)
            
            # append the value to the output Buffer
            output_buffer.append(val)
            
            # get the Buffer at the specific index given from the heap
            buff = buff_list[file_ind]
            
            # checks to see if we have read through all of the data in the Buffer
            if buff.already_read >= buff.buffer.size:
                """
                If yes we need to increase the offset by 1 to prevent reading data we've already processes.
                Then we need to replace the old data in the buffer with the next 2^23 bytes of data.
                Next set already_read to 0 as we haven't read anything from the replenished Buffer.
                finally if the size after refilling is 0 that means we have read all the data in the temp file and we shouldn't 
                add anything to the heap so we can break out of this iteration of the loop.
                """
                buff.offset += 1
                with open(buff.file, 'rb') as f:
                    f.seek(buff.offset * 8 * buff.max_size)
                    buff.buffer = np.fromfile(f, dtype=np.uint64, count=buff.max_size)

                buff.already_read = 0

                if buff.buffer.size == 0:
                    continue
                    
            # Get the next value from the Buffer that was specified earlier
            val = buff.buffer[buff.already_read]
            buff.already_read += 1
            
            # add the next tuple to the heap.
            next_tuple = (val, file_ind)
            heapq.heappush(heap, next_tuple)
            
            
        """
        Flush the output Buffer to the output file and then reset the output Buffer
        """
        with open(sys.argv[2], 'ab') as to_file:
            output_buffer = np.array(output_buffer, dtype=np.uint64)
            output_buffer.tofile(to_file)
            output_buffer = []


    """
    A check to see if after are loops have concluded that there is data left in the output Buffer. If so 
    flush the output Buffer to the output file and then reset the output Buffer.
    """
    if len(output_buffer) > 0:
        with open(sys.argv[2], 'ab') as to_file:
            output_buffer = np.array(output_buffer, dtype=np.uint64)
            output_buffer.tofile(to_file)
            output_buffer = []
            

# Call split_n_sort
split_n_sort()

# Call split_n_sort
merge()

location = os.getcwd()
path = os.path.join(location, "temp")

shutil.rmtree(path)
