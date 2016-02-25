import os
import distutils.spawn
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
try:
    os.mkdir(os.path.join('log'))
except:
    pass
os.chdir(BASE_DIR)
os.execv(distutils.spawn.find_executable('gunicorn'), [
    'gunicorn', 'explorer:app', '-k', 'gevent',
    '--access-logfile', 'log/access.log',
    '--error-logfile', 'log/error.log'] + sys.argv[1:])
