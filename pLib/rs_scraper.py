# Custom module to with functions to retrieve data from websites
# Author: Rupinder Singh 
from bs4 import BeautifulSoup #html parser
import requests
import zipfile
import subprocess as sp
import socket
import smtplib
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def get_linked_urls(in_url):
    """ Will retrieve html parse and return 
    all links present under <a  href tag
    """
    r = requests.get(in_url).content.decode('utf-8','ignore') # get HTML
    soup = BeautifulSoup(r,'html.parser') 
    
    linked_url = []
    for link in soup.find_all('a'):
        linked_url.append(str(link.get('href')))
    
    return linked_url

def download_file(in_url, fn):
    """ Will download file from specified URL
    """    
    r = requests.get(in_url, stream=True,  verify=False, allow_redirects=True)
    if r.status_code == 200:
        CHUNK_SIZE = 8192
        bytes_read = 0
        with open(fn, 'wb') as f:
            itrcount=1
            for chunk in r.iter_content(CHUNK_SIZE):
                itrcount = itrcount+1
                f.write(chunk)
                bytes_read += len(chunk)
    r.close()

def find_item(in_list, match_string, match_all=True, negate=False, case_sensitive=False):
    """ Retrieves items in_list that have string present in match_strings
        Default is to return items that match all strings in match_string
        match_all = 1 returns item satisfying AND match logic with items in match_strings
        match_all != 1 returns item satisfying OR match logic with items in match_strings
        negate = 1 will return nonmatching items instead of matching ones

    """
    out_list = []
    #make sure match_sting is a list
    if isinstance(match_string,str):
        match_string = [match_string]

    for s in in_list:
        if not case_sensitive:
            if match_all:
                flag = all(ms.lower() in s.lower() for ms in match_string)
            else:
                flag = any(ms.lower() in s.lower() for ms in match_string)
        else:
            if match_all:
                flag = all(ms in s for ms in match_string)
            else:
                flag = any(ms in s for ms in match_string)

        if negate:
            flag = not flag

        if flag:
            out_list.append(s)

    return out_list

def get_namelist_in_zip(zfn):
    with open(zfn, 'rb') as z:
        zfile = zipfile.ZipFile(z)
        return zfile.namelist()

def extract_file_from_zip(zfn, in_fn, ext_pth="./"):
    """ Extracts file (in_file) from zipfile (zfn) 
        and saves it under ext_pth
    """
    with zipfile.ZipFile(zfn, 'r') as zip_ref:
        zip_ref.extract(in_fn,ext_pth)

def write_file_to_zip(zfn, in_fn, append = False):
    """ Writes file (in_file) to zipfile (zfn) 
    """
    if append:
        w_or_a = 'a' #append
    else:
        w_or_a = 'w' #write/overwrite

    with zipfile.ZipFile(zfn, w_or_a, zipfile.ZIP_DEFLATED, allowZip64=True) as zip_ref:
        zip_ref.write(in_fn, basename(in_fn))

def file_exists_in_path(fn, pth):
    """ Returns true if file (fn) exists in path (pth)
    """
    cmd = ["ls "+pth.replace('\ ',' ').replace(" ","\ ")] #make path unix friendly
    p = sp.Popen(cmd, stdout = sp.PIPE, stderr= sp.PIPE, shell=True)
    stdout, stderr = p.communicate()
    fnL = stdout.decode("utf-8").split('\n')
    return fn in fnL

def remove_file(fn):
    """ Removes file, fn (pythonic form of unix rm command)
    """
    cmd = ["rm "+fn.replace('\ ',' ').replace(" ","\ ")] #make path unix friendly
    p = sp.Popen(cmd, stdout = sp.PIPE, stderr= sp.PIPE, shell=True)
    stdout,stderr = p.communicate()
    return stderr.decode('utf8')

def move_file(fn1, fn2):
    """ Moves file from fn to fn2 (pythonic form of unix mv command) 
    """
    cmd = ["mv "+fn1.replace('\ ',' ').replace(" ","\ ")+" "+fn2.replace('\ ',' ').replace(" ","\ ")] #make path unix friendly
    p = sp.Popen(cmd, stdout = sp.PIPE, stderr= sp.PIPE, shell=True)
    stdout,stderr = p.communicate()
    return stderr.decode('utf8')

def mirror_dir(remote_cfg, local_dir):
    """ Mirrors files from remote_dir to local_dir """
    cmd_connect = "open -u "+remote_cfg['user']+','+remote_cfg['pw']+' '+remote_cfg['host']+':'+remote_cfg['port']
    cmd_filelist = "ls "+remote_cfg['dir'] 
    cmd_mirror = "mirror --verbose --delete --only-newer --parallel=3 "+remote_cfg['dir']+' '+local_dir
    cmd = 'lftp<<EOF\n' \
        + 'echo "1. Connecting to" '+ remote_cfg['host'] + ';' \
        + cmd_connect + ';' \
        + 'echo "2. File list at" '+ remote_cfg['dir'] + ';' \
        + cmd_filelist + ';' \
        + 'echo "3. Mirroring to" '+ local_dir + ';' \
        + cmd_mirror + ';' \
        + 'bye;\n' \
        'EOF'
    p = sp.Popen(cmd, stdout = sp.PIPE, stderr= sp.PIPE, shell=True)
    stdout,stderr = p.communicate()
    return stderr.decode('utf8')

def get_hostname():
    """ Moves file from fn to fn2 (pythonic form of unix mv command) 
    """
    return socket.gethostname()

def send_email(FROM,TO,SUBJECT,HTML,FILES=None):
    """ This function can be used to send email to "to" from "from" with subject and supplied html
        Optional. If a list of files (FILES) with their full path are supplied, they will be added as attachments to the email
    """
    # Create message container - the correct MIME type is multipart/alternative.
    MSG = MIMEMultipart('alternative')
    MSG['Subject'] = SUBJECT
    MSG['From'] = FROM
    MSG['To'] = TO

    part = MIMEText(HTML,'html')
    MSG.attach(part)
    
    for f in FILES or []:
        with open(f, 'rb') as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part.add_header('Content-Disposition', 'attachment', filename=basename(f))
        MSG.attach(part)

    SERVER = 'localhost'
    s = smtplib.SMTP(SERVER)
    s.sendmail(FROM, TO.split(","), MSG.as_string())
    s.quit()
