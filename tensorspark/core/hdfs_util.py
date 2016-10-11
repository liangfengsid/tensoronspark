 # httpfs_utils.py
#
# Provides HDFS access via httpfs using Python's requests package.
 
 
import datetime
import requests
try:
    import simplejson as json
except ImportError:
    import json
 
 
###################################################################################################
# Helper functions                                                                                #
###################################################################################################
 
def _get_max_str_len_(filestatuses, key):
    """ Returns the max string value length for a list of dictionaries with 'field' as key.
 
    This is used to pretty print directory listings.
 
    INPUT
    -----
    filestatus : list of dicts
        The FileStatuses dictionary returned by the liststatus method.
    key : str
        The key for which we wish to find the maximum length value.
 
    OUTPUT
    ------
    int : The length of the longest value.
    """
    return max([len(str(B[key])) for B in filestatuses['FileStatuses']['FileStatus']])
 
 
def _perm_long_str_(type_str, perm_str):
    """ Forms the long string version of the permission string.
 
    INPUT
    -----
    type_str : str
        The type of object as given by list, e.g., 'FILE' or 'DIRECTORY'.
    perm_str : str
        The short form (numeric) version of the permission string.
 
    OUTPUT
    ------
    str : The long form version of the permission string.
    """
    # Determine if a directory is represented.
    if type_str == 'DIRECTORY':
        perm_str_long = 'd'
    else:
        perm_str_long = '-'
    # Convert the permission string to long letter form.
    for n in perm_str:
        L = [int(i) for i in list(bin(int(n)).split('0b')[1].zfill(3))]
        if L[0]:
            perm_str_long += 'r'
        else:
            perm_str_long += '-'
        if L[1]:
            perm_str_long += 'w'
        else:
            perm_str_long += '-'
        if L[2]:
            perm_str_long += 'x'
        else:
            perm_str_long += '-'
 
    return perm_str_long
 
 
def make_webhdfs_url(host, user, hdfs_path, op, port=50070):
    """ Forms the URL for httpfs requests.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    op : str
        The httpfs operation string. E.g., 'GETFILESTATUS'.
    port : int
        The port to use for httpfs connections.
 
    OUTPUT
    ------
    str : The string to use for an HTTP request to httpfs.
    """
    url = 'http://' + host + ':' + str(port) + '/webhdfs/v1'
    url += hdfs_path + '?user.name=' + user + '&op=' + op
 
    return url
 
 
###################################################################################################
# Functions                                                                                       #
###################################################################################################
 
def append(host, user, hdfs_path, filename, port=50070):
    """ Appends contents of 'filename' to 'hdfs_path' on 'user'@'host':'port'.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file to be appended to in HDFS.
    filename : str
        The file with contents being appended to hdfs_path. Can be a local file or a full path.
    port : int : default=50070
        The port to use for httpfs connections.
    """
    # Form the URL.
    url = make_webhdfs_url(
        host=host,
        user=user,
        hdfs_path=hdfs_path,
        op='APPEND&data=true',
        port=port
    )
    headers = {
        'Content-Type':'application/octet-stream'
    }
 
    resp = requests.post(url, data=open(filename,'rb'), headers=headers)
    if resp.status_code != 200:
        resp.raise_for_status
 
 
def appends(host, user, hdfs_path, content, port=50070):
    """ Appends 'content' to 'hdfs_path' on 'user'@'host':'port'.
 
    This method is like 'append', but takes a string as input instead of a file name.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file to be appended to in HDFS.
    content : str
        The contents being appended to hdfs_path.
    port : int : default=50070
        The port to use for httpfs connections.
    """
    # Form the URL.
    url = make_webhdfs_url(
        host=host,
        user=user,
        hdfs_path=hdfs_path,
        op='APPEND&data=true',
        port=port
    )
    headers = {
        'Content-Type':'application/octet-stream'
    }
 
    resp = requests.post(url, data=content, headers=headers)
    if resp.status_code != 200:
        resp.raise_for_status
 
 
def copy_to_local(host, user, hdfs_path, filename, port=50070):
    """ Copies the file at 'hdfs_path' on 'user'@'host':'port' to 'filename' locally.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file in HDFS.
    port : int : default=50070
        The port to use for httpfs connections.
    perms : str or int : default=775
        The permissions to use for the uploaded file in HDFS.
    """
    # Form the URL.
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op='OPEN', port=port)
 
    # Form and issue the request.
    resp = requests.get(url, stream=True)
 
    if resp.status_code == 200:
        with open(filename, 'wb') as f_p:
            for chunk in resp:
                f_p.write(chunk)
    else:
        resp.raise_for_status
 
 
def exists(host, user, hdfs_path, port=50070):
    """ Returns True if 'hdfs_path' (full path) exists in HDFS at user@host:port via httpfs.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    port : int
        The port to use for httpfs connections.
 
    OUTPUT
    ------
    Boolean : True if 'hdfs_path' exists and can be accessed by 'user'; False otherwise.
    """
    op = 'GETFILESTATUS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
    # Get the JSON response using httpfs; stores as a Python dict
    resp = requests.get(url)
    # If a 404 was returned, the file/path does not exist
    if resp.status_code == 404:
        return False
    # If a 200 was returned, the file/path does exist
    elif resp.status_code == 200:
        return True
    # Something else - raise status, or if all else fails return None
    else:
        resp.raise_for_status()
        return None
 
 
def get_blocksize(host, user, hdfs_path, port=50070):
    """ Returns the HDFS block size (bytes) of 'hdfs_path' in HDFS at user@host:port via httpfs.
 
    The returned block size is in bytes. For MiB, divide this value by 2**20=1048576.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    port : int
        The port to use for httpfs connections.
 
    OUTPUT
    ------
    int/long : The block size in bytes.
    """
    op = 'GETFILESTATUS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
    # Get the JSON response using httpfs; stores as a Python dict
    resp = requests.get(url)
    # If a 200 was returned, the file/path exists
    if resp.status_code == 200:
        return resp.json()['FileStatus']['blockSize']
    # Something else - raise status, or if all else fails return None
    else:
        resp.raise_for_status()
 
 
def get_size(host, user, hdfs_path, port=50070):
    """ Returns the size (bytes) of 'hdfs_path' in HDFS at user@host:port via httpfs.
 
    The returned block size is in bytes. For MiB, divide this value by 2**20=1048576.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    port : int
        The port to use for httpfs connections.
 
    OUTPUT
    ------
    int/long : The size in bytes.
    """
    op = 'GETFILESTATUS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
    # Get the JSON response using httpfs; stores as a Python dict
    resp = requests.get(url)
    # If a 200 was returned, the file/path exists
    if resp.status_code == 200:
        return resp.json()['FileStatus']['length']
    # Something else - raise status, or if all else fails return None
    else:
        resp.raise_for_status()
 
 
def info(host, user, hdfs_path, port=50070):
    """ Returns a dictionary of info for 'hdfs_path' in HDFS at user@host:port via httpfs.
 
    This method is similar to 'liststatus', but only displays top-level information. If you need
    info about all of the files and subdirectories of a directory, use 'liststatus'.
 
    The returned dictionary contains keys: group, permission, blockSize, accessTime, pathSuffix,
    modificationTime, replication, length, ownder, type.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    port : int
        The port to use for httpfs connections.
 
    OUTPUT
    ------
    Dictionary : Information about 'hdfs_path'
    """
    op = 'GETFILESTATUS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
    # Get the JSON response using httpfs; stores as a Python dict
    resp = requests.get(url)
    # If a 200 was returned, the file/path exists
    if resp.status_code == 200:
        return resp.json()
    # Something else - raise status, or if all else fails return None
    else:
        resp.raise_for_status()
 
 
def liststatus(host, user, hdfs_path, port=50070):
    """ Returns a dictionary of info for 'hdfs_path' in HDFS at user@host:port via httpfs.
 
    Returns a dictionary of information. When used on a file, the returned dictionary contains a
    copy of the dictionary returned by 'info.' When used on a directory, the returned dictionary
    contains a list of such dictionaries.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    port : int
        The port to use for httpfs connections.
 
    OUTPUT
    ------
    Dictionary : Information about 'hdfs_path'
    """
    op = 'LISTSTATUS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
    # Get the JSON response using httpfs; stores as a Python dict
    resp = requests.get(url)
    # If a 200 was returned, the file/path exists
    if resp.status_code == 200:
        return resp.json()
    # Something else - raise status, or if all else fails return None
    else:
        resp.raise_for_status()
 
 
def ls(host, user, hdfs_path, port=50070):
    """ Print info for 'hdfs_path' in HDFS at user@host:port via httpfs.
 
    A print function intended for interactive usage. Similar to 'ls -l' or 'hdfs dfs -ls'.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file or directory being checked.
    port : int
        The port to use for httpfs connections.
    """
    op = 'LISTSTATUS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
    # Get the JSON response using httpfs; stores as a Python dict
    resp = requests.get(url)
    # If a 200 was returned, the file/path exists. Otherwise, raise error or exit.
    if resp.status_code != 200:
        resp.raise_for_status()
    else:
        filestatuses = resp.json()
        for obj in filestatuses['FileStatuses']['FileStatus']:
            obj_str = _perm_long_str_(type_str=obj['type'],perm_str=obj['permission'])
            obj_str += '%*s' % (
                _get_max_str_len_(filestatuses, 'replication')+3,
                obj['replication']
            )
            obj_str += '%*s' % (
                _get_max_str_len_(filestatuses, 'owner')+3,
                obj['owner']
            )
            obj_str += '%*s' % (
                _get_max_str_len_(filestatuses, 'group')+2,
                obj['group']
            )
            obj_str += '%*s' % (
                _get_max_str_len_(filestatuses, 'length')+4,
                obj['length']
            )
            obj_str += '%21s' % (
                datetime.datetime.utcfromtimestamp(
                    obj['modificationTime']/1000
                ).isoformat().replace('T',' ')
            )
            obj_str += ' ' + hdfs_path + '/' + obj['pathSuffix']
 
            print "%s" % obj_str
 
 
def mkdir(host, user, hdfs_path, port=50070):
    """ Creates the directory 'hdfs_path' on 'user'@'host':'port'.
 
    Directories are created recursively.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The path of the directory to create in HDFS.
    port : int : default=50070
        The port to use for httpfs connections.
    """
    op = 'MKDIRS'
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op=op, port=port)
 
    # Make the request
    resp = requests.put(url)
    # If a 200 was returned, the file/path exists
    if resp.status_code == 200:
        return resp.json()
    # Something else - raise status, or if all else fails return None
    else:
        resp.raise_for_status()
 
 
def put(host, user, hdfs_dir, filename, port=50070, perms=644):
    """ Puts 'filename' into 'hdfs_path' on 'user'@'host':'port'.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the location to place the file in HDFS.
    filename : str
        The file to upload. Can be a local file or a full path.
    port : int : default=50070
        The port to use for httpfs connections.
    perms : str or int : default=775
        The permissions to use for the uploaded file in HDFS.
    """
    # Get the file name without base path.
    filename_short = filename.split('/')[-1]
    hdfs_path = hdfs_dir + '/' +filename_short
    # Form the URL.
    url = make_webhdfs_url(
        host=host,
        user=user,
        hdfs_path=hdfs_path,
        op='CREATE&overwrite=true',
        port=port
    )
    headers = {
        'Content-Type':'application/octet-stream'
    }
    #files = {'file': open(filename,'rb')}

    resp = requests.put(url, allow_redirects=False)
    if resp.status_code != 307:
        resp.raise_for_status()

    upload_url = resp.headers['Location']
    file = open(filename, 'rb')
    resp2 = requests.put(upload_url, data=file, headers=headers)
    file.close()
    if resp2.status_code != 201:
        resp2.raise_for_status()
 
 
def read(host, user, hdfs_path, port=50070):
    """ Reads file at 'hdfs_path' on 'user'@'host':'port'.
 
    This method allows the contents of a file in HDFS to be read into memory in Python.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file in HDFS.
    port : int : default=50070
        The port to use for httpfs connections.
    perms : str or int : default=775
        The permissions to use for the uploaded file in HDFS.
 
    OUTPUT
    ------
    Text of the file.
    """
    # Form the URL.
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op='OPEN', port=port)
 
    # Form and issue the request.
    resp = requests.get(url)
 
    if resp.status_code != 200:
        resp.raise_for_status
 
    return resp.text
 

def get(host, user, hdfs_path, local_dir):
    """
    @Param:
    hdfs_path: the list containing the paths of the files on hdfs
    """
    if isinstance(hdfs_path, list):
        filename = [path.split('/')[-1] for path in hdfs_path]
        paths = ' '.join(hdfs_path)
    else:
        filename = hdfs_path.split('/')[-1]
        paths = hdfs_path
    
    if isinstance(filename, list):
        returned_paths = [local_dir+'/'+name for name in filename]
    else:
        returned_paths = local_dir + '/' + filename

    import os
    """
    Delete the local files of the same name before getting them from HDFS.
    """
    if isinstance(returned_paths, list):
        for file in returned_paths:
            try:
                os.remove(file)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
    else:
        try:
            os.remove(returned_paths)
        except OSError as e:
            pass
            # if e.errno != e.ENOENT:
                # raise


    cmd = 'hadoop fs -get %s %s/' % (paths, local_dir)
    os.system(cmd)

    if isinstance(filename, list):
        return [local_dir+'/'+name for name in filename]
    else:
        return local_dir + '/' + filename

 
def read_json(host, user, hdfs_path, port=50070):
    """ Reads JSON file at 'hdfs_path' on 'user'@'host':'port' and returns a Python dict.
 
    This method reads the contents of a JSON file in HDFS into Python as a dictionary.
 
    INPUT
    -----
    host : str
        The host to connect to for httpfs access to HDFS. (Can be 'localhost'.)
    user : str
        The user to use for httpfs connections.
    hdfs_path : str
        The full path of the file in HDFS.
    port : int : default=50070
        The port to use for httpfs connections.
    perms : str or int : default=775
        The permissions to use for the uploaded file in HDFS.
 
    OUTPUT
    ------
    Text of the file interpreted in JSON as a Python dict.
    """
    # Form the URL.
    url = make_webhdfs_url(host=host, user=user, hdfs_path=hdfs_path, op='OPEN', port=port)
 
    # Form and issue the request.
    resp = requests.get(url)
 
    if resp.status_code != 200:
        resp.raise_for_status
 
    return json.loads(requests.get(url).text)
