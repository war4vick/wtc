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
option_parser.add_option('-o', '--protocol',default='udp', help='send via UDP instead of TCP default:"tcp"')
option_parser.add_option('-e', '--db_exp', default='.wsp', help='database expansion default:".wsp"')
option_parser.add_option('-l', '--metrics_len', type=int, default=100, help='database expansion default:".wsp"')
<<<<<<< HEAD
option_parser.add_option('-d', '--db_name', default='none',type='string', help='Whisper database dir name default:"wtc"')
option_parser.add_option( '--debug', default=False, action='store_true', help='debug')
(options, args) = option_parser.parse_args()
log_name = ''

=======
option_parser.add_option('-d', '--db_name', default='whisper',type='string', help='Whisper database dir name default:"whisper"')
option_parser.add_option( '--debug', default=False, action='store_true', help='debug')
(options, args) = option_parser.parse_args()
log_name = ''
>>>>>>> fd0b16fd5b480d5d182829373b368b32259fb76a
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

<<<<<<< HEAD
def build_messages(path,offset):
    try:
        (timestamp, value) = struct.unpack(whisper.pointFormat,map[offset:offset + whisper.pointSize])
        prefix=read_path(path)
        sender = Sender(options.server,protocol=options.protocol)
        message = sender.build_message(prefix, value, timestamp)
        logging.debug(' Read prefix:%s'%(prefix)+' Timestamp:%d'%(timestamp,)+' Value:%d'%(value,))
        return message
    except Exception as e:
        print('Error build_messages from [build_messages(path,offset)] ' + str(e))


=======
>>>>>>> fd0b16fd5b480d5d182829373b368b32259fb76a
def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

def dump_archives(archives,path):
    print(path)
    for i, archive in enumerate(archives):
        offset = archive['offset']
        print(' Read Archive %d'%(i,)+' retention:%d'%(archive['retention'],)+' secondsPerPoint:%d'%(archive['secondsPerPoint'],)+' Total point :%d '%(archive['points'],))
<<<<<<< HEAD
=======
        #sleep(2)
>>>>>>> fd0b16fd5b480d5d182829373b368b32259fb76a
        mass=""
        num_point=0
        try:
            for point in xrange(archive['points']):
<<<<<<< HEAD
                mass += build_messages(path,offset)
                num_point += 1
                offset += whisper.pointSize
                #print(num_point)
                if num_point >= options.metrics_len or archive['points'] == point+1 :
                    progress(point, archive['points'], status='')
                    sender = Sender(options.server,protocol=options.protocol)
                    sender.send_mass(mass)
                    mass=""
                    num_point=0
                    #print(' Readed point :%d '%(point+1,))
            print(" It took "+ str(time.time()-time_start)+" seconds.")
        except Exception as e:
            #print(e)
            logging.error('Error occurred ' + str(e)+': Read from Archive %d:'%(i,)+ " " + path )
            print('Error occurred ' + str(e)+': Read from Archive %d:'%(i,)+ " " + path )

=======
                if  i == 3 :
                    sys.exit()
                (timestamp, value) = struct.unpack(whisper.pointFormat,map[offset:offset + whisper.pointSize])
                prefix=read_path(path)
                logging.debug(' Read prefix:%s'%(prefix)+' Timestamp:%d'%(timestamp,)+' Value:%d'%(value,))
                sender = Sender(options.server,protocol=options.protocol)
                mass += sender.build_message(prefix, value, timestamp) #metric, value, timestamp, tags={}
                num_point += 1
                offset += whisper.pointSize
                if num_point >= options.metrics_len or archive['points'] == point+1 :
                    progress(point, archive['points'], status='')
                    sender.send_mass(mass)
                    mass=""
                    num_point=0
            print(' Readed point :%d '%(point+1,))
            print(" It took "+ str(time.time()-time_start)+" seconds.")
        except Exception as e:
            print(e)
            logging.error('Error occurred ' + str(e)+': Read from Archive %d:'%(i,)+ " " + path )
>>>>>>> fd0b16fd5b480d5d182829373b368b32259fb76a
if __name__ == '__main__':
    time_start = time.time()
    print("Start time "+ datetime.utcfromtimestamp(time_start).strftime('%Y-%m-%d %H:%M:%S'))
    procs=[]
<<<<<<< HEAD
    print()
    #path.split(os.sep)[-1])
    if os.path.isfile(path) == True :
        map = mmap_file(path)
        header = read_header(map)
        dump_archives(header['archives'],str(path))

    elif os.path.isdir(path) == True :
        for root, dirs, files in os.walk(path):     #find all files in dir with path  root = full path to dir default:/var/lib/graphite/whisper/files = all file in all dir          default:*name*.wsp

            for file  in files :                                                        #main cycle
                if file.endswith(options.db_exp):
                #print('Write DB %s'%(read_path(root+"/"+file),))
                    map = mmap_file(root+"/"+file)
                    header = read_header(map)
                    try:
                        print()
                        dump_archives(header['archives'],root+"/"+file)
                    except Exception as e:
                        logging.error('Error occurred ' + str(e))
    else :
        print("Error not right path")
        sys.exit()
=======
    #find all files in dir with path
    #root = full path to dir default:/var/lib/graphite/whisper/
    #files = all file in all dir default:*name*.wsp
    for root, dirs, files in os.walk(path):
        #main cycle

        for file  in files :
            if file.endswith(options.db_exp):
                #print('Write DB %s'%(file,))
                map = mmap_file(root+"/"+file)
                header = read_header(map)
                try:
                    dump_archives(header['archives'],root+"/"+file)
                except IOError as e:
                    logging.error('Error occurred ' + str(e))

>>>>>>> fd0b16fd5b480d5d182829373b368b32259fb76a
   #logging.info('Success!!!!')
logging.info ("It took "+ str(time.time()-time_start)+" seconds.")
logging.info ("List processing complete.")
