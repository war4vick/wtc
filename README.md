# wtc
wisper_to_clickhouse

pip install -r requirements.txt

run comand : python copy.py  /var/lib/graphite/wisper/ -s localhost -p 2003 -u udp -e .wsp -d whisper
