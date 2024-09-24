mkdir -p -- "build/dist"

cd "./build" || exit

cp "../main.py" "./"
cp "../config.json" "./dist"
cp "../README.MD" "./dist"

python -m venv zusi
./zusi/Scripts/activate
pip3 install tqdm
pip3 install pyinstaller
cp "D:\SteamLibrary\steamapps\common\ZUSI 3 - Aerosoft Edition\64bit\ZusiFtdEditor.64.exe" "./"
pyinstaller --onefile --icon=ZusiFtdEditor.64.exe main.py

cd ".." || exit

mkdir -p -- "./deploy"

cp "./build/dist/main.exe" "./deploy/"
cp "./build/dist/config.json" "./deploy/"
cp "./build/dist/README.MD" "./deploy/"

rm -rf "./build"
