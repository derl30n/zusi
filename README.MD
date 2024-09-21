### zugdiensteAktualisieren.bat

```
"$path\python.exe" "main.py"
pause

```

### zugienstdatenbank.bat

```
start "" "$path\SQLite.exe" "zugdienste.db"
```

### Executable:

`python -m venv zusi`

`.\zusi\Scripts\activate`

`pip3 install tqdm`

`python .\main.py` to check if it runs as expected in new virtual environtment

`pip3 install pyinstaller` install the pyinstaller if not done yet

`copy "D:\SteamLibrary\steamapps\common\ZUSI 3 - Aerosoft Edition\64bit\ZusiFtdEditor.64.exe"` copy the file as icon for the exe

`pyinstaller --onefile --icon=ZusiFtdEditor.64.exe main.py` create the exe

`.\dist\main.exe` run the exe !!dont forget to provide the config.json!!
