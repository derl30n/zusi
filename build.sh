python -m venv zusi
.\zusi\Scripts\activate
pip3 install tqdm
pip3 install pyinstaller
cp "D:\SteamLibrary\steamapps\common\ZUSI 3 - Aerosoft Edition\64bit\ZusiFtdEditor.64.exe" "./"
pyinstaller --onefile --icon=ZusiFtdEditor.64.exe main.py
cp "config.json" "./dist"
cp "README.MD" "./dist"
rm -rf "./zusi"
rm -rf "./build"
rm -rf "main.spec"
rm -rf "ZusiFtdEditor.64.exe"
