import os,mmap,time,struct,signal,sys,subprocess,logging,whisper,optparse
from optparse import OptionParser
from time import sleep
import numpy as np
from graphyte import *
from multiprocessing import Process
import status
from datetime import datetime

option_parser = optparse.OptionParser(usage='''%prog path to dir -s localhost -p 2003 -u udp -e .wsp -d whisper''')
option_parser.add_option('-s', '--server', default='localhost', help='hostname of server to send  default:"localhost"')
option_parser.add_option('-p', '--port', type=int, default=2003 ,help='port to send message to default:"2003"')
option_parser.add_option('-o', '--protocol',default='tcp', help='send via UDP instead of TCP default:"tcp"')
option_parser.add_option('-e', '--db_exp', default='.wsp', help='database expansion default:".wsp"')
option_parser.add_option('-l', '--metrics_len', type=int, default=100, help='database expansion default:".wsp"')
option_parser.add_option('-d', '--db_name', default='none',type='string', help='Whisper database dir name default:"wtc"')
option_parser.add_option('-x', '--timestamp_start', type=int, default=0, help=' ')
option_parser.add_option('-z', '--timestamp_end', type=int, default=0, help=' ')
option_parser.add_option( '--debug', default=False, action='store_true', help='debug')

(options, args) = option_parser.parse_args()
log_name = ''

if options.debug:
    logging.basicConfig(
        level=logging.DEBUG ,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        )
else:
    logging.basicConfig(
        filename='error_log_'+str(time.time())+'.log',
        level=logging.ERROR ,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        )

if len(args) != 1:
  option_parser.error("require one input file name")
else:
  path = args[0]

def mmap_file(filename):
  fd = os.open(filename, os.O_RDONLY)
  map = mmap.mmap(fd, os.fstat(fd).st_size, prot=mmap.PROT_READ)
  os.close(fd)
  return map

def read_path(path):
    full_path=os.path.abspath(path)                              #transformation path like /var/lib/graphite/whisper/carbon/metric/name.wsp to carbon prefix carbon.metric.name
    pat=full_path.split(os.sep)
    lis=np.array(pat)
    end=int(len(pat))

    path_full=""
    llen=-1+-len(options.db_exp)
    if options.db_name == "none" :                               #definition of all element in 'pat'
        start=int((pat.index(os.path.basename(os.getcwd())))+1)
    elif options.db_name in lis :
        start=int((pat.index(options.db_name)))                 #search in a list by name of base and setting a start index to create a prefix
                                                  #databases expansion length like ".wsp"  == 4
    for x in range(start,end):                                  #creating a metric prefix relative to the database directory
        path_full+=(lis[x])+"."
    return (path_full[:llen])                                   #remove databases expansion with in prefix


def read_header(map):
  try:
    (aggregationType, maxRetention, xFilesFactor, archiveCount) \
      = struct.unpack(whisper.metadataFormat, map[:whisper.metadataSize])
  except (struct.error, ValueError, TypeError):
    raise whisper.CorruptWhisperFile("Unable to unpack header")

  archives = []
  archiveOffset = whisper.metadataSize

  for i in xrange(archiveCount):
    try:
      (offset, secondsPerPoint, points) = struct.unpack(
        whisper.archiveInfoFormat,
        map[archiveOffset:archiveOffset + whisper.archiveInfoSize]
      )
    except (struct.error, ValueError, TypeError):
      raise whisper.CorruptWhisperFile("Unable to read archive %d metadata" % i)

    archiveInfo = {
      'offset': offset,
      'secondsPerPoint': secondsPerPoint,
      'points': points,
      'retention': secondsPerPoint * points,
      'size': points * whisper.pointSize,
    }
    archives.append(archiveInfo)
    archiveOffset += whisper.archiveInfoSize

  header = {
    'aggregationMethod': whisper.aggregationTypeToMethod.get(aggregationType, 'average'),
    'maxRetention': maxRetention,
    'xFilesFactor': xFilesFactor,
    'archives': archives,
  }
  return header

def current_timestamp():
    if not options.timestamp_end :
        return time.time()
    else :
        return options.timestamp_end

def find_timestamp(timestamp):
    if  timestamp <= current_timestamp() and  timestamp >= options.timestamp_start :
        return True
    else :
        return False

def build_messages(path,offset):
    try:
        (timestamp, value) = struct.unpack(whisper.pointFormat,map[offset:offset + whisper.pointSize])
        if find_timestamp(timestamp):
            #print (timestamp)
            prefix=read_path(path)
            sender = Sender(options.server,protocol=options.protocol)
            message = sender.build_message(prefix, value, timestamp)
            logging.debug(' Read prefix:%s'%(prefix)+' Timestamp:%d'%(timestamp,)+' Value:%d'%(value,))
            return message
    except Exception as e:
        print('Error build_messages from [build_messages(path,offset)] ' + str(e))

def progress(count, total, status=''):
    bar_len = 55
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write("\033[0;32m")
    if percents == 100:
        sys.stdout.write("\033[0;32m")
    else:
        sys.stdout.write("\033[2;33m")
    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def dump_archives(archives,path):
    sys.stdout.write("\033[0;32m")
    print(">Read from:%s"%(path,))
    sys.stdout.write("\033[1;36m")
    print(">>Write as:%s"%(read_path(path),))
    #sleep(1)
    for i, archive in enumerate(archives):
        offset = archive['offset']
        sys.stdout.write("\033[0;0m")
        mass=""
        num_point=0
        read_point=0
        try:
            for point in xrange(archive['points']):
                data = build_messages(path,offset)
                if  data :
                    mass += data
                    num_point += 1
                    read_point +=1
                offset += whisper.pointSize
                #print(num_point)
                #print(mass[1])
                if num_point >= options.metrics_len or archive['points'] == point+1 :
                    progress(point+1, archive['points'], status='')
                    sender = Sender(options.server,protocol=options.protocol)
                    sender.send_mass(mass)
                    mass=""

                    num_point=0
            print('>>>Readed point :%d '%(read_point,))
            print(">>>It took "+ str(time.time()-time_start)+" seconds.")
        except Exception as e:
            #print(e)
            logging.error('Error occurred ' + str(e)+': Read from Archive %d:'%(i,)+ " " + path )
            print('Error occurred ' + str(e)+': Read from Archive %d:'%(i,)+ " " + path )

if __name__ == '__main__':
    sys.stdout.write("\033[1;31m")
    time_start = time.time()
    print("Start time "+ datetime.utcfromtimestamp(time_start).strftime('%Y-%m-%d %H:%M:%S'))
    procs=[]
    #path.split(os.sep)[-1])
    if os.path.isfile(path) == True :
        map = mmap_file(path)
        header = read_header(map)
        dump_archives(header['archives'],str(path))

    elif os.path.isdir(path) == True :
        for root, dirs, files in os.walk(path):     #find all files in dir with path  root = full path to dir default:/var/lib/graphite/whisper/files = all file in all dir          default:*name*.wsp

            for file  in files :                                                        #main cycle
                if file.endswith(options.db_exp):

                    map = mmap_file(root+"/"+file)
                    header = read_header(map)
                    try:
                        dump_archives(header['archives'],root+"/"+file)
                    except Exception as e:
                        logging.error('Error occurred ' + str(e))
    else :
        print("Error not right path")
        sys.exit()

   #logging.info('Success!!!!')
logging.info ("It took "+ str(time.time()-time_start)+" seconds.")
logging.info ("List processing complete.")
