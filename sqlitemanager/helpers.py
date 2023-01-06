import os
import ntpath
from shutil import copyfile

def get_localpath():
    """set the paths to the users documents folder"""

    local_path = os.path.join("~", "Documents")
    path = os.path.expanduser(local_path)

    return path

def split_complete_path(complete_path):

    path, tail = ntpath.split(complete_path)

    if tail == "":
        path, tail = ntpath.split(complete_path[:-1])

    filename = tail.split('.', 1)[0]
    
    # print(f"path {path}")
    # print(f"filename {filename}")

    return path, filename

def show_files(path):
    """Show all files in a folder"""

    # Create file list
    filelist = []
    # check if directory already exists, if not cancel opening
    if not os.path.exists(path):
        print(f"couldnt find path: {path}")
        return filelist
        
    # Iterate over sqlite files
    for filename in os.listdir(path):
        if filename.endswith(".sqlite*"): 
            name = os.path.splitext(filename)[0]
            filelist.append(name)

    return path, filelist

def saveas_file(srcfile, dstfile, srcpath, dstpath, extension):

    src = os.path.join(srcpath, srcfile + extension)
    dst = os.path.join(dstpath, dstfile + extension)

    if os.path.isfile(src):

        if os.path.exists(dstpath) == False:
            os.makedirs(dstpath)

        if os.path.isfile(dst):
            print(f"error path destination: {dst} already exists")

        else:
            print(f"copying file: {src} to {dst}")
            copyfile(src, dst)

    else:
        print(f"Source path does not exist: {src}")