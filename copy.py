import os,mmap,time,struct,signal,sys,subprocess,logging,whisper,optparse
from optparse import OptionParser
import numpy as np
from graphyte import *
from multiprocessing import Process


option_parser = optparse.OptionParser(usage='''%prog path to dir -s localhost -p 2003 -u udp -e .wsp -d whisper''')
option_parser.add_option('-s', '--server', default='localhost', help='hostname of server to send  default:"localhost"')
option_parser.add_option('-p', '--port', type=int, default=2003 ,help='port to send message to default:"2003"')
option_parser.add_option('-o', '--protocol', action='store_true',default='udp', help='send via UDP instead of TCP default:"tcp"')
option_parser.add_option('-e', '--db_exp', default='.wsp', action='store_true',help='database expansion default:".wsp"')
option_parser.add_option('-d', '--db_name', default='whisper', action='store',type='string', help='Whisper database dir name default:"whisper"')
option_parser.add_option( '--debug', default=False, action='store_true', help='debug')
(options, args) = option_parser.parse_args()

if options.debug:
    logging.basicConfig(
        level=logging.DEBUG ,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        )
else:
    logging.basicConfig(
        filename='error_log.log',
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

#transformation path like /var/lib/graphite/whisper/carbon/metric/name.wsp to carbon prefix carbon.metric.name
def read_path(path):
  pat=path.split(os.sep)
  lis=np.array(pat)
  name=np.array(file)
  #definition of all element in 'pat'
  end=int(len(pat))
  #search in a list by name of base and setting a start index to create a prefix
  start=int((pat.index(options.db_name))+1)
  path_full=""
  #databases expansion length like ".wsp"  == 4
  llen=-1+-len(options.db_exp)
  #creating a metric prefix relative to the database directory
  for x in range(start,end):
    path_full+=(lis[x])+"."
  #remove databases expansion with in prefix
  return (path_full[:llen])

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

def dump_archives(archives,path):
    for i, archive in enumerate(archives):
        offset = archive['offset']
        for point in xrange(archive['points']):
            (timestamp, value) = struct.unpack(whisper.pointFormat,map[offset:offset + whisper.pointSize])
        if  timestamp != 0:
            prefix=read_path(path)
            logging.debug('Read prefix:%s'%(prefix)+' Value:%d'%(value,))
            try:
                sender = Sender(options.server,protocol=options.protocol)
                sender.send('%s'%(prefix,), int(value), timestamp=int(timestamp))
            except IOError as e:
                logging.error('Error occurred ' + str(e) +' Prefix:%s'%(prefix,)+' Value:%d'%(value))

if __name__ == '__main__':
    print('start')
    time_start = time.time()
    procs=[]
    #find all files in dir with path
    #root = full path to dir default:/var/lib/graphite/whisper/
    #files = all file in all dir default:*name*.wsp
    for root, dirs, files in os.walk(path):
        #main cycle

        for file  in files :
            if file.endswith(options.db_exp):
                print('Write DB %s'%(file,))
                map = mmap_file(root+"/"+file)
                header = read_header(map)
                try:
                    #run send current metrics databases to read like :/var/lib/graphite/whisper/*/*.wsp as subprocess
                    proc = Process(target=dump_archives, args=(header['archives'],root+"/"+file))
                    procs.append(proc)
                    proc.start()

                except IOError as e:
                    logging.error('Error occurred ' + str(e))

    print("It took "+ str(time.time()-time_start)+" seconds.")    #logging.info('Success!!!!')
    logging.info ("It took "+ str(time.time()-time_start)+" seconds.")
    logging.info ("List processing complete.")
