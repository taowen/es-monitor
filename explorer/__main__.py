import os
import distutils.spawn
import sys

os.execv(distutils.spawn.find_executable('gunicorn'), ['gunicorn', 'explorer:app', '-k', 'gevent'] + sys.argv[1:])
