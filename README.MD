
# Step-by-step Setup and Usage Guide

## Navigation:
1. [Download](#download)
2. [Setup](#setup)
   - [I: Folder Structure Overview](#i-folder-structure-overview)
   - [II: Editing `config.json`](#ii-editing-configjson)
   - [III: Running the Executable](#iii-running-the-executable)
3. [Usage](#usage)
   - [IV: Opening the Database File](#iv-opening-the-database-file)
   - [V: Example](#v-example)
4. [Developer Information and customization](#dev-info)

---

## Download

Download the build from the [latest release](https://github.com/derl30n/zusi/releases/latest) or click [here](https://github.com/derl30n/zusi/releases/latest/download/release.zip).

---

## Setup

### I: Folder Structure Overview
After downloading the latest build and extracting the zip file, your folder should look like this:
![Folder Structure](https://i.imgur.com/agk5WrX.png)

---

### II: Editing `config.json`
Open the `config.json` with an editor of your choice. I recommend [Notepad++](https://notepad-plus-plus.org/downloads/).

![Config JSON](https://i.imgur.com/UbfBd35.png)

The relevant section is `paths`, where you can configure:
1. Shipped schedules (installation directory)
2. User-created schedules (documents directory)

_If you haven’t created any custom timetables, the documents folder might not exist._

---

### III: Running the Executable
After setting up `config.json`, run the executable.

![Running EXE](https://i.imgur.com/hNbEts5.png)

If there's an error, like the directory for custom timetables not being found, it's likely because no custom timetables have been created yet.

_Note: The console window closes automatically upon execution. To keep it open, run the exe from the terminal._

Once finished, the database file should be visible:
![Database File](https://i.imgur.com/QbnYmn1.png)

---

## Usage

### IV: Opening the Database File
To explore the database file, use a database viewer like [sqlite](https://sqlitebrowser.org/dl/).

![SQLite Viewer](https://i.imgur.com/yXwCSo8.png)

Click on the "Browse Data" tab to filter and sort through all drivable jobs in Zusi.

![Browse Data](https://i.imgur.com/458IQnL.png)
![Browse Data](https://imgur.com/DFz2wSZ.png)

---

### V: Example

Route: Hamburg - Kassel  
Loco: BR 412 (ICE 4)

![Example Route](https://i.imgur.com/7VQ8dg1.png)
![Example Loco](https://i.imgur.com/RXm0UjN.png)
![Service Details](https://i.imgur.com/kXYdglS.png)

---

## Dev Info

### zugdiensteAktualisieren.bat

```
"$path\python.exe" "main.py"
pause

```
---
### zugienstdatenbank.bat

```
start "" "$path\SQLite.exe" "zugdienste.db"
```

To open db file, use a db viewer like [sqlite](https://sqlitebrowser.org/dl/)

---

### Build executable:

`python -m venv zusi`

`.\zusi\Scripts\activate`

`pip3 install tqdm`

`python .\main.py` to check if it runs as expected in new virtual environment

`pip3 install pyinstaller` install the pyinstaller if not done yet

`copy "D:\SteamLibrary\steamapps\common\ZUSI 3 - Aerosoft Edition\64bit\ZusiFtdEditor.64.exe"` copy the file as icon for the exe

`pyinstaller --onefile --icon=ZusiFtdEditor.64.exe main.py` create the exe

`.\dist\main.exe` run the exe !!don't forget to provide the config.json!!

---
