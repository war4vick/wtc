# wtc
wisper_to_clickhouse

pip install -r requirements.txt

if workxdir have any *.wsp file
run comand : python copy.py $(pwd) -s localhost -p 2003 -o tcp -e .wsp -d wtc
