import json
import os
import re
import sqlite3
import xml.etree.ElementTree as Et
from dataclasses import dataclass, field
from datetime import datetime
from tqdm import tqdm
from enum import Enum


# Error Codes
# 99 -> less than two entries in trn | at least two are required for a valid schedule
#  1 -> failed health check (invalid trn start or end)
#  0 -> no valid start/entry found in timetable
#  2 -> no remaining timetable entries after start has been defined


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
    INVALID = -1
    OFFENE_STRECKE = 0
    BETRIEBSSTELLE = 1
    GBF = 2
    PBF = 3


class Entry:
    __slots__ = (
        'name',
        'timeArr',
        'timeDep',
        'isPlannedStop',
        'isValid',
        'hasArrivalTime',
        'flag'
    )

    name: str
    timeArr: datetime | None
    timeDep: datetime | None
    isPlannedStop: bool
    isValid: bool
    hasArrivalTime: bool
    flag: Flags

    def __init__(self, name: str, timeArr: datetime | None, timeDep: datetime | None):
        self.name = name
        self.timeArr = timeArr
        self.timeDep = timeDep
        self.isPlannedStop = all([name, timeArr, timeDep])
        self.isValid = all([name, timeDep])
        self.hasArrivalTime = timeArr is not None

        self.flag = Flags.INVALID
        self._setFlag()

    def __cmp__(self, other) -> bool:
        return self.name == other.name

    def _setFlag(self) -> None:
        if self.name is None:
            return

        # Typically only one kind of flag should be settable / condition True

        # if self._nameContains(["GSMR", "ICON"]):
        #     self.flags.append(Flags.INVALID)

        if self._nameContains(["SBK", "BK", "ESIG", "ZSIG", "ASIG", "ABZW", "ÜST", "VSIG"]):
            self.flag = Flags.OFFENE_STRECKE
            return

        if self._nameContains(["BFT", "BBF"]):
            self.flag = Flags.BETRIEBSSTELLE
            return

        if self._nameContains(["HP", "PBF", "HBF"]):
            self.flag = Flags.PBF
            return

        if self._nameContains(["GBF", "RBF"]):
            self.flag = Flags.GBF
            return

    def _nameContains(self, flags: list[str]) -> bool:
        return any(keyword.lower() in self.name.lower() for keyword in flags)

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
    __slots__ = (
        'isTurnAround',
        'runningDistance'
    )

    isTurnAround: bool
    runningDistance: int

    def __init__(self, rawEntry):
        dist = rawEntry.get('FplLaufweg')
        name = rawEntry.findall('FplName')
        arr = rawEntry.findall('FplAnk')
        dep = rawEntry.findall('FplAbf')

        self.isTurnAround = rawEntry.find('FplRichtungswechsel') is not None
        self.runningDistance = int(float(dist)) if dist is not None else 0

        nameStr = name[0].get('FplNameText') if len(name) > 0 else None
        arrStr = self.getTime(arr[0].get('Ank')) if len(arr) > 0 else None
        depStr = self.getTime(dep[0].get('Abf')) if len(dep) > 0 else arrStr

        super().__init__(name=nameStr, timeArr=arrStr, timeDep=depStr)

    def override(self, other: Entry) -> None:
        self.name = other.name
        self.timeArr = other.timeArr
        self.timeDep = other.timeDep
        self.isValid = True
        self.isPlannedStop = False

        # TODO: do we need to re-set flags in this case??
        self.flags = []
        self._setFlags()


class EntryTrn(Entry):
    __slots__ = (
        'hasEvent',
        'isTurnAround'
    )

    hasEvent: bool
    isTurnAround: bool

    def __init__(self, rawEntry):
        name = rawEntry.get('Betrst')
        arr = self.getTime(rawEntry.get('Ank'))
        dep = self.getTime(rawEntry.get('Abf'))

        self.isTurnAround = rawEntry.get('FzgVerbandAktion') == "2"
        self.hasEvent = len(rawEntry.findall('Ereignis')) > 0

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
    _plannedStopps: list[EntryTrn]
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

        trn_rows = trn.findall('FahrplanEintrag')

        # don't process super short services, not worth it
        if len(trn_rows) < 2:
            # print(99, trn.findall('BuchfahrplanRohDatei')[0].get('Dateiname'))
            return

        # if len(timetable_rows) < 2 or len(trn_rows) < 2:
        #     # print(99, trn.findall('BuchfahrplanRohDatei')[0].get('Dateiname'))
        #     return

        timetable_list = schedule.findall('Buchfahrplan')
        initial_timetable = timetable_list[0]
        self._gattung = initial_timetable.get('Gattung')
        self._zugnr = initial_timetable.get('Nummer')
        self._br = initial_timetable.get('BR')
        self._laenge = int(float(initial_timetable.get('Laenge')))
        self._masse = int(int(initial_timetable.get('Masse')) / 1000)
        self._zuglauf = initial_timetable.get('Zuglauf')

        for timetable in timetable_list[1:]:
            self._zugnr = f"{self._zugnr}_{timetable.get('Nummer')}"
            self._zuglauf = f"{self._zuglauf} -> {timetable.get('Zuglauf')}"

        serviceSplit = service.split('\\')
        trackSplit = serviceSplit[0].split("/")

        self._country = trackSplit[-2]
        self._route = trackSplit[-1]
        self._fahrplan = serviceSplit[-2]

        self._constructRoute(trn_rows, timetable_list)

    def _setEndFromEndPoint(self, end_trn: EntryTrn, timetable_list: list) -> None:
        # TODO: perhaps refactor, to only fetch the running distance and store it on the trn entry
        timetable_rows: list = [entry for row in timetable_list for entry in row.findall("FplZeile")]
        timetable_rows.reverse()

        for row in timetable_rows:
            # TODO: perhaps this might be possible to not create so many objects
            # if not end_trn.name == row.findall('FplName'):
            #     continue
            #
            # dist = row.get('FplLaufweg')
            # runningDistance = int(float(dist)) if dist is not None else 0
            # end_trn.setRunningDistance(runningDistance)
            #
            # return
            # <<- end.trn.isValid = False

            entry_timetable = EntryTimetable(row)

            # print(end_trn.name, entry_timetable.name, end_trn.name == entry_timetable.name)

            if not end_trn.name == entry_timetable.name:
                continue

            self._end = entry_timetable
            break

    def _constructRoute(self, trn_rows: list, timetable_list: list) -> None:
        # trn file holds all necessary information, except for running distance
        route: list[EntryTrn] = self._getValidEntryTrnAsList(trn_rows)

        # don't add a service that has no valid start and end points
        if len(route) < 2:
            print("_constructRoute: route<2")
            return

        self._start: EntryTrn = route.pop(0)
        end: EntryTrn = route.pop(-1)
        self._plannedStopps = [entry for entry in route if entry.isPlannedStop]

        self._setEndFromEndPoint(end, timetable_list)

        # there might be a change that we do not find the entry from trn in the timetable
        if not self._end.isValid:
            # print(self._fahrplan, self._gattung, self._zugnr, self._end.name, end.name, self._end.flag, "\n", ", ".join(stopp.name for stopp in route))
            print(f"{self._fahrplan}, {self._gattung}, {self._zugnr}, [{self._end.name}], [{end.name}], {self._end.flag}")

            print("something went wrong finding the pair")
            return

        # TODO: do we need to check for duplicates like previously?

        self._changeFlagOnDeviatingNames()

        self.isValid = self._start.isValid and self._end.isValid

    @staticmethod
    def _getValidEntryTrnAsList(trn_rows: list) -> list[EntryTrn]:
        res: list[EntryTrn] = []

        for row in trn_rows:
            entry_trn = EntryTrn(row)

            # invalid entries are ignored since they hold no valuable information
            if not entry_trn.isValid:
                continue

            res.append(entry_trn)

        return res

    def _changeFlagOnDeviatingNames(self) -> None:
        # we are trying to use a naming scheme on zusi services,
        # where services that do not star/terminate at the actual locations have fictional names, hence not match actual position

        # TODO: perhaps freight trains can profit from this as well?
        if not self._isPassengerTrain:
            return

        if self._zuglauf is None:
            return

        split: list[str] = self._zuglauf.split("-")

        if len(split) < 2:
            return

        if self._start.flag in [Flags.PBF, Flags.INVALID]:
            self._start.flag = Flags.PBF if self._start.name == getFilteredText(split[0]) else Flags.OFFENE_STRECKE

        if self._end.flag in [Flags.PBF, Flags.INVALID]:
            self._end.flag = Flags.PBF if self._end.name == getFilteredText(split[1]) else Flags.OFFENE_STRECKE

    def getAsDict(self) -> dict:
        duration = self._end.timeDep - self._start.timeDep
        dv = 0 if duration.seconds == 0 else int((self._end.runningDistance / duration.seconds) * 3.6)

        return {
            "gattung": self._gattung,
            "zugnr": self._zugnr,
            "begin": datetime.strftime(self._start.timeDep, "%H:%M"),
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
    with open(f'{prefix}{filename}.json', "r") as json_data_file:
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


def main():
    data: list[dict] = extrapolateDataFromZusi()
    keys: dict.keys = data[0].keys()
    values: list[tuple] = [tuple(entry.values()) for entry in data]

    createDatabaseWithData(keys, values)


if __name__ == '__main__':
    main()
