import os
import glob
import tarfile
import multiprocessing as mp

from pathlib import Path


def unziptar(fullpath):
    """worker unzips one file"""
    print('extracting... {}'.format(fullpath))
    tar = tarfile.open(fullpath)
    new_folder_name = os.path.join(Path(fullpath).parent, Path(fullpath).stem)
    tar.extractall(new_folder_name)
    tar.close()


def extract_subdir_list(path):
    return glob.glob(path + '/*/')


def read_files(path, extension='*.*', recursive=False):
    if path is None:
        return []
    if recursive:
        subdirs = glob.glob(path + '/*/')
        files = []
        for subdir in subdirs:
            # files+=glob.glob(subdir + '/' + extension)
            if len(extract_subdir_list(subdir)) > 0:
                files += read_files(subdir, extension, recursive=recursive)
            else:
                files += glob.glob(subdir + '/' + extension)
        return files
    else:
        return glob.glob(os.path.join(path, extension))


def fanout_unziptar(path):
    """create pool to extract all"""
    # my_files = []
    my_files = read_files(path=path)
    # for root, dirs, files in os.walk(path):
    #     for i in files:
    #         if i.endswith(".tar"):
    #             my_files.append(os.path.join(root, i))

    pool = mp.Pool(min(mp.cpu_count(), len(my_files)))  # number of workers
    pool.map(unziptar, my_files, chunksize=1)
    pool.close()


if __name__ == "__main__":
    path = '/home/hyunkoo/DATA/Datasets/nuScenes/Full_dataset_v1.0/Trainval'
    fanout_unziptar(path)
    print('extraction has completed')
