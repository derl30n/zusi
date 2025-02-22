import json
import os
import re
import sqlite3
import xml.etree.ElementTree as Et
from dataclasses import dataclass, field
from datetime import datetime
from tqdm import tqdm
from enum import Enum

# Formate:
# einfacher name = durchgehender string e.g. "Salzkotten"
# komplexer name = mehrere str e.g. "Aachen HBF" oder auch "Aachen West" oder "Au (Sieg)"
# gbf = name + str e.g. "Hildesheim Gbf"
# hbf = name + str e.g. "Hildesheim Hbf"
# pbf = name + str e.g. ""
# selbstblöcke = str int e.g. "SBK 18"
# abzweige = str + str + (str) e.g. "Abzw Berliner Straße" oder "Abzw Heide". 3. str kann / muss nicht
# haltepunkt = komplexer oder einfacher name + str e.g. "Bad St Peter-Ording Hp"
# bft = str + komplexer name e.g. "Bft Au-Hirblinger Straße"
# bk = str + name e.g. "Bk Buchberg"
# überleitstelle = str + name e.g. "Üst Veerßen"


class Flags(Enum):
    INVALID = -2
    TIMETABLE_INFO = -1
    UNKNOWN = 0
    OFFENE_STRECKE = 1
    BETRIEBSSTELLE = 2
    GBF = 3
    PBF = 4


class Entry:
    __slots__ = (
        'name',
        'timeArr',
        'timeDep',
        'flag',
        'hasEvent',
        'isTurnAround',
        'runningDistance',
        'isEbulaInfo'
    )

    name: str
    timeArr: datetime | None
    timeDep: datetime | None
    isTurnAround: bool
    runningDistance: int
    hasEvent: bool
    flag: Flags
    isEbulaInfo: bool

    def __init__(
            self,
            name: str,
            timeArr: datetime | None,
            timeDep: datetime | None,
            isTurnAround: bool = False,
            runningDistance: int = 0,
            isEbulaInfo: bool = False
    ):
        self.name = name
        self.timeArr = timeArr
        self.timeDep = timeDep
        self.isTurnAround = isTurnAround
        self.runningDistance = runningDistance
        self.isEbulaInfo = isEbulaInfo

        # we do this so that we can always read timeDep, no influence on functionality
        if all([name, timeArr]) and self.timeDep is None:
            self.timeDep = timeArr

        self.flag = Flags.INVALID

        if self.isEbulaInfo:
            self.flag = Flags.TIMETABLE_INFO
            return

        if self.name is None:
            if self.timeArr and self.timeDep:
                self.flag = Flags.OFFENE_STRECKE

            return

        if self._matchesEbulaInfoPattern():
            self.flag = Flags.TIMETABLE_INFO
            return

        flag: Flags = stations.get(self.name.lower())
        if flag is not None:
            self.flag = flag
            return

        if self._nameContains(["SBK", "BK", "ESIG", "ZSIG", "ASIG", "ABZW", "ÜST", "VSIG", "LZB", "NACH", "BKSIG", "LZB-BK", "BKSIG", "STRECKENENDE", "ENDE"]):
            self.flag = Flags.OFFENE_STRECKE
            return

        if self._nameContains(["BBF", "ÜST"]):
            self.flag = Flags.BETRIEBSSTELLE
            return

        if self._nameContains(["HP", "PBF", "HBF", "BF", "HST", "BFT"]):
            self.flag = Flags.PBF
            return

        if self._nameContains(["GBF", "RBF"]):
            self.flag = Flags.GBF
            return

        self.flag = Flags.UNKNOWN

    def _matchesEbulaInfoPattern(self) -> bool:
        return bool(re.match(r"^-\s.*\s-$", self.name.lower()))

    def _nameContains(self, keyword_list: list[str]) -> bool:
        return any(keyword.lower() in self.name.lower().split(" ") for keyword in keyword_list)

    @staticmethod
    def getTime(timeString: str | None) -> datetime | None:
        if timeString is None:
            return None

        try:
            return datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.strptime(timeString, '%Y-%m-%d')


class EntryPlaceholder(Entry):
    def __init__(self):
        super().__init__(name="", timeArr=None, timeDep=None)


class EntryTimetable(Entry):
    def __init__(self, rawEntry):
        dist = rawEntry.get('FplLaufweg')
        name = rawEntry.findall('FplName')
        arr = rawEntry.findall('FplAnk')
        dep = rawEntry.findall('FplAbf')
        isEbulaInfo = len(rawEntry.findall('FplIcon')) > 0

        nameStr = name[0].get('FplNameText') if len(name) > 0 else None
        arrStr = self.getTime(arr[0].get('Ank')) if len(arr) > 0 else None
        depStr = self.getTime(dep[0].get('Abf')) if len(dep) > 0 else None
        isTurnAround = rawEntry.find('FplRichtungswechsel') is not None
        runningDistance = int(float(dist)) if dist is not None else 0

        super().__init__(name=nameStr, timeArr=arrStr, timeDep=depStr, isTurnAround=isTurnAround, runningDistance=runningDistance, isEbulaInfo=isEbulaInfo)


class EntryTrn(Entry):
    def __init__(self, rawEntry):
        name = rawEntry.get('Betrst')
        arr = self.getTime(rawEntry.get('Ank'))
        dep = self.getTime(rawEntry.get('Abf'))

        super().__init__(name=name, timeArr=arr, timeDep=dep)


class Service:
    __slots__ = (
        'isValid',
        '_start',
        '_end',
        '_plannedStopps',
        '_turnarounds',
        '_hasEvent',
        '_gattung',
        '_zugnr',
        '_br',
        '_laenge',
        '_masse',
        '_zuglauf',
        '_isPassengerTrain',
        '_country',
        '_route',
        '_fahrplan'
    )

    isValid: bool

    _start: EntryTrn | EntryPlaceholder
    _end: EntryTimetable | EntryPlaceholder
    _plannedStopps: list[EntryTimetable]
    _turnarounds: int
    _hasEvent: bool

    _gattung: str
    _zugnr: str
    _br: str
    _laenge: int
    _masse: int
    _zuglauf: str
    _isPassengerTrain: bool

    _country: str
    _route: str
    _fahrplan: str

    def __init__(self, service: str, schedule, trn):
        self.isValid = False
        self._start = EntryPlaceholder()
        self._end = EntryPlaceholder()
        self._plannedStopps = []
        self._turnarounds = 0
        self._hasEvent = False

        self._isPassengerTrain = bool(trn.get("Zugtyp"))
        self._zuglauf = trn.get("Zuglauf")

        if self._zuglauf is None:
            return

        trn_rows = trn.findall('FahrplanEintrag')
        timetable_list = schedule.findall('Buchfahrplan')
        timetable_rows = [entry for row in timetable_list for entry in row.findall("FplZeile")]

        # don't process super short services, not worth it
        if len(trn_rows) < 2 or len(timetable_rows) < 2:
            return

        initial_timetable = timetable_list[0]
        self._gattung = initial_timetable.get('Gattung')
        self._zugnr = initial_timetable.get('Nummer')
        self._br = initial_timetable.get('BR')
        self._laenge = int(float(initial_timetable.get('Laenge')))
        self._masse = int(int(initial_timetable.get('Masse')) / 1000)

        for timetable in timetable_list[1:]:
            self._zugnr = f"{self._zugnr}_{timetable.get('Nummer')}"
            self._zuglauf = f"{self._zuglauf} -> {timetable.get('Zuglauf')}"

        serviceSplit = service.split('\\')
        trackSplit = serviceSplit[0].split("/")

        self._country = trackSplit[-2]
        self._route = trackSplit[-1]
        self._fahrplan = serviceSplit[-2]

        entryTimetableList: list[EntryTimetable] = self._getEntryTimetableAsList(timetable_rows)

        link = service.replace(" ", "%20").replace("\\", "/")

        # don't add a service that has no valid start and end points
        if len(entryTimetableList) < 2:
            return

        self._constructRoute(entryTimetableList, trn_rows, link)

    def _setStartTag(self, timetableStart: EntryTimetable, closestPoint: EntryTimetable) -> None:
        # Annahme:
        # 1. zuglauf start == trn start -> gegebenes trn start Flag
        # 2. entry timetable (mit arr und dep) < 800m FplLaufweg -> gegebenes timetable entry Flag
        # 3. nahegelegender entry timetable (mit PBF, GBF Flag) < 800m FplLaufweg -> gegebenes timetable entry Flag
        # sonst immer Flags.OFFENE_STRECKE

        zuglauf_start = self._zuglauf.split(" - ")[0]

        # 1.
        if zuglauf_start in self._start.name:
            return

        # 2
        if timetableStart is not None and timetableStart.runningDistance < 800:
            self._start.flag = timetableStart.flag
            return

        # 3 - kein planmäßiger stopp, jedoch starten wir im PBF oder GBF
        if closestPoint is not None and closestPoint.runningDistance < 800:
            # 3.1 naher punkt hat selben namen wie zuglauf
            if zuglauf_start in closestPoint.name:
                self._start.flag = closestPoint.flag
                return

            # 3.2 naher punkter hat selben namen wie start trn
            if closestPoint.name == self._start.name:
                return

        self._start.flag = Flags.OFFENE_STRECKE

    def _constructRoute(self, entryTimetableList: list[EntryTimetable], trn_rows: list, link: str) -> None:
        entryTimetableListDepArrTimes: list[EntryTimetable] = [entry for entry in entryTimetableList if all([entry.timeArr, entry.timeDep])]

        self._start = EntryTrn(trn_rows[0])
        self._setStartTag(
            entryTimetableListDepArrTimes[0] if entryTimetableListDepArrTimes else None,
            next((entry for entry in entryTimetableList if any([entry.flag.PBF, entry.flag.GBF])), None)
        )

        self._plannedStopps = self._filter_consecutive_duplicates(entryTimetableListDepArrTimes)
        self._end = next((entry for entry in reversed(entryTimetableList) if entry.timeDep is not None), None)

        if self._end is None:
            return

        self._hasEvent = any(len(row.findall('Ereignis')) > 0 for row in trn_rows)

        self.isValid = all([self._start.timeArr, self._end.timeDep])

    @staticmethod
    def _filter_consecutive_duplicates(entryTimetableListDepArrTimes: list[EntryTimetable]) -> list[EntryTimetable]:
        filtered_stops = []
        last_name = None

        for entry in entryTimetableListDepArrTimes:
            # we don't need to check for names since all PBF and GBF have names
            if entry.flag not in [Flags.PBF, Flags.GBF]:
                continue

            if last_name == entry.name:
                continue

            # This is 99% our starting point, so do not add
            if last_name is None and entry.runningDistance < 800:
                continue

            filtered_stops.append(entry)
            last_name = entry.name

        return filtered_stops

    def _getEntryTimetableAsList(self, timetable_rows: list) -> list[EntryTimetable]:
        res: list[EntryTimetable] = []

        for row in timetable_rows:
            entry_timetable = EntryTimetable(row)

            self._turnarounds += entry_timetable.isTurnAround

            if entry_timetable.flag == Flags.TIMETABLE_INFO or entry_timetable.flag == Flags.INVALID:
                continue

            res.append(entry_timetable)

        return res

    def getAsDict(self) -> dict:
        duration = (self._end.timeArr or self._end.timeDep) - self._start.timeDep
        dv = 0 if duration.seconds == 0 else int((self._end.runningDistance / duration.seconds) * 3.6)

        return {
            "art": "P" if self._isPassengerTrain else "C",
            "gattung": self._gattung,
            "zugnr": self._zugnr,
            "begin": datetime.strftime(self._start.timeArr, "%H:%M"),
            "fahrzeit": str(duration),
            "br": self._br,
            "laenge": self._laenge,
            "masse": self._masse,
            "nhalte": len(self._plannedStopps),
            "ev": self._hasEvent,
            "w1": self._turnarounds,
            "start": self._start.flag.name,
            "ende": self._end.flag.name,
            "s_km": int(self._end.runningDistance / 1000),
            "dv": dv,
            "country": self._country,
            "route": self._route,
            "fahrplan": self._fahrplan,
            "aufgleispunkt": self._start.name,
            "zuglauf": self._zuglauf,
            "halte": ", ".join(stopp.name for stopp in self._plannedStopps)
        }


@dataclass(frozen=True)
class Datatypes:
    timetable: str
    service: str


@dataclass(frozen=True)
class Config:
    paths: list = field(default_factory=list, compare=False)
    datatype: Datatypes = field(default_factory=Datatypes)
    exclusionKeywords: list = field(default_factory=list, compare=False)


def getFilteredText(text: str) -> str:
    return re.sub(r"^[A-Za-z]+\s?\d*\s", "", text).strip()


def readFromJsonFile(filename: str, prefix: str = "") -> dict:
    with open(f'{prefix}{filename}.json', "r", encoding="utf-8") as json_data_file:
        return json.load(json_data_file)


def getTimetablesFromZusiFiles(config: Config) -> list:
    timetables = []

    try:
        for cfgPath in config.paths:
            for country in os.listdir(cfgPath):  # looping countries
                if country.lower() in config.exclusionKeywords:
                    continue

                for route in os.listdir(f'{cfgPath}/{country}'):  # looping routes
                    timetables.extend(
                        [f.path[:-(len(config.datatype.timetable) + 1)] for f in
                         os.scandir(f'{cfgPath}/{country}/{route}')
                         if config.datatype.timetable == f.path[-len(config.datatype.timetable):]]
                    )
    except FileNotFoundError as e:
        print(e)

    return timetables


def isServiceValid(service: str, flagged_words: list[str]) -> bool:
    return not any([x.lower() in service.lower() for x in flagged_words])


def getDataFromTimetables(timetables: list, config: Config) -> list[dict]:
    result: list[dict] = []
    errors: list[str] = []

    for timetable in tqdm(timetables, desc="Durchsuche Fahrpläne nach Zugdiensten"):
        for service in [f.path for f in os.scandir(timetable) if
                        config.datatype.service == f.path[-len(config.datatype.service):]]:
            if not isServiceValid(service, config.exclusionKeywords):
                continue

            root = Et.parse(service).getroot()

            try:
                trn_root = Et.parse(f'{service[:-13]}trn').getroot()
            except FileNotFoundError:
                continue

            trn_zug = trn_root.findall('Zug')[0]

            if not isServiceValid(trn_zug.get('FahrplanGruppe'), config.exclusionKeywords):
                continue

            extractedService = Service(service, root, trn_zug)

            if not extractedService.isValid:
                errors.append(service)
                continue

            result.append(extractedService.getAsDict())

    print(f"{len(errors)} ungültige Zugdienste ausgeschlossen.")

    return result


def loadStationDefinition() -> dict[str, Flags]:
    station_dict = readFromJsonFile("stations")

    return {key: Flags[value.upper()] for key, value in station_dict.items()}


def extrapolateDataFromZusi() -> list[dict]:
    res = readFromJsonFile("config")

    config = Config(
        res["paths"],
        Datatypes(**res["datatype"]),
        res["exclusionKeywords"]
    )

    timetables = getTimetablesFromZusiFiles(config)
    print(f'{len(timetables)} Fahrpläne gefunden.')

    result = getDataFromTimetables(timetables, config)
    print(f"{len(result)} Zugdienste gefunden.")

    return result


def createDatabaseWithData(keys: dict.keys, data: list[tuple]):
    con = sqlite3.connect("zugdienste.db")
    cur = con.cursor()

    keys_string: str = ", ".join(keys)
    values_string: str = ", ".join("?" for _ in range(len(keys)))

    for table_name in ["_00_latest", f"_{datetime.now().strftime("%d_%m_%Y")}"]:
        try:
            cur.execute(f"DROP TABLE {table_name}")
        except sqlite3.OperationalError:
            pass

        cur.execute(
            f"CREATE TABLE {table_name}({keys_string})")
        cur.executemany(f"INSERT INTO {table_name} VALUES({values_string})", data)
        con.commit()

    print("Zugdienste in Datenbank eingetragen.")


stations: dict[str, Flags] = loadStationDefinition()


def main():
    data: list[dict] = extrapolateDataFromZusi()
    keys: dict.keys = data[0].keys()
    values: list[tuple] = [tuple(entry.values()) for entry in data]

    createDatabaseWithData(keys, values)


if __name__ == '__main__':
    main()
