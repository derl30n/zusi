import json
import os
import sqlite3
import xml.etree.ElementTree as Et
from dataclasses import dataclass, field
from datetime import datetime
from tqdm import tqdm


# Error Codes
# 99 -> less than two entries in trn | at least two are required for a valid schedule
#  1 -> failed health check (invalid trn start or end)
#  0 -> no valid start/entry found in timetable
#  2 -> no remaining timetable entries after start has been defined


class Entry:
    __slots__ = (
        'name',
        'timeArr',
        'timeDep',
        'isPlannedStop',
        'isValid',
        'hasArrivalTime'
    )

    name: str
    timeArr: datetime | None
    timeDep: datetime | None
    isPlannedStop: bool
    isValid: bool
    hasArrivalTime: bool

    def __init__(self, name: str, timeArr: datetime | None, timeDep: datetime | None):
        self.name = name
        self.timeArr = timeArr
        self.timeDep = timeDep
        self.isPlannedStop = all([name, timeArr, timeDep])
        self.isValid = all([name, timeDep])
        self.hasArrivalTime = timeArr is not None

    def __cmp__(self, other) -> bool:
        return self.name == other.name

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


class EntryTrn(Entry):
    __slots__ = 'hasEvent'

    hasEvent: bool

    def __init__(self, rawEntry):
        name = rawEntry.get('Betrst')
        arr = self.getTime(rawEntry.get('Ank'))
        dep = self.getTime(rawEntry.get('Abf'))

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
        '_country',
        '_route',
        '_fahrplan'
    )

    isValid: bool

    _start: EntryTimetable | EntryPlaceholder
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

        timetable_list = schedule.findall('Buchfahrplan')
        timetable_rows = [entry for row in timetable_list for entry in row.findall("FplZeile")]
        trn_rows = trn.findall('FahrplanEintrag')

        if len(timetable_rows) < 2 or len(trn_rows) < 2:
            # print(99, trn.findall('BuchfahrplanRohDatei')[0].get('Dateiname'))
            return

        self._findStart(timetable_rows, trn_rows)

        if not self._start.isValid:
            return

        if not self.isValid:
            return

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

    def _findStart(self, timetable_rows: list, trn_rows: list) -> None:
        start_trn = EntryTrn(trn_rows[0])
        end_trn = EntryTrn(trn_rows[-1])

        if not start_trn.isValid or not end_trn.isValid:
            # print(1, start_trn.name, start_trn.timeDep)
            return

        # On rare occasions timeArr is not defined on first entry in trn
        if not start_trn.hasArrivalTime:
            start_trn.timeArr = start_trn.timeDep

        running_index_timetable: int = -1
        for i, row in enumerate(timetable_rows):
            entry = EntryTimetable(row)

            if not entry.isValid:
                continue

            self._start = entry
            running_index_timetable = i + 1

            break

        if not self._start.isValid:
            # print(0)
            return

        if (start_trn.timeDep - start_trn.timeArr).seconds > 60:
            self._start.isPlannedStop = True

        if not self._start.name == start_trn.name:
            self._start.override(start_trn)
            running_index_timetable = 0

        # exclude already "checked" rows -> only pass unchecked rows
        remaining_timetable_rows = timetable_rows[running_index_timetable:]

        if len(remaining_timetable_rows) == 0:
            # print(2)
            return

        self._findPlannedStopps(remaining_timetable_rows, trn_rows[1:])

    def _findPlannedStopps(self, timetable_rows: list, trn_rows: list) -> None:
        trn_current_index: int = -1
        last_valid_index: int = 0

        for entry in trn_rows:
            trn_current_index += 1
            entry_trn = EntryTrn(entry)

            if not entry_trn.isValid:
                continue

            index: int = 0
            # TODO: fix potential index out of range error
            for i, entry_tt in enumerate(timetable_rows[last_valid_index:]):
                entry_timetable = EntryTimetable(entry_tt)

                # memorize index so we are not checking the same invalid entries over and over
                if not entry_timetable.isValid:
                    index = i
                    continue

                # prevent the timetable list from "overtaking" the trn list
                if entry_timetable.timeDep > entry_trn.timeDep:
                    break

                index = i

                if not entry_timetable.isPlannedStop:
                    if entry_timetable.name == entry_trn.name or entry_trn.name in entry_timetable.name:
                        break
                    continue

                # Ensure the planned stop fitting our time frame has also the correct name
                if entry_timetable.name != entry_trn.name and entry_trn.name not in entry_timetable.name:
                    continue

                self._plannedStopps.append(entry_timetable)
                self._turnarounds += entry_timetable.isTurnAround

                break

            last_valid_index += index + 1

            # if there is an event set, we can no longer check reliably for planned stopps
            if entry_trn.hasEvent:
                self._hasEvent = True
                break

        isLastTrnIndexUsed = len(trn_rows) == trn_current_index + 1

        self._findEnd(timetable_rows, isLastTrnIndexUsed, trn_rows[-1])

    def _findEnd(self, timetable_rows: list, isLastTrnIndexUsed: bool, trn_last) -> None:
        # is schedule completely populated? -> last entry = end
        if isLastTrnIndexUsed == 0 and len(self._plannedStopps) > 0:
            self._end = self._plannedStopps[-1]
            self._validate()

            return

        for row in reversed(timetable_rows[-5:]):
            entry = EntryTimetable(row)

            if not entry.isValid:
                continue

            self._end = entry
            break

        trn_end = EntryTrn(trn_last)

        if not self._end.isValid or self._end.name != trn_end.name:
            self._end = EntryTimetable(timetable_rows[-1])
            self._end.override(trn_end)

        self._validate()
        return

    def _validate(self) -> None:
        self.isValid = self._start.isValid and self._end.isValid

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
            "v3": self._start.isPlannedStop,
            "a3": self._end.isPlannedStop,
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

    table_name = f"_{datetime.now().strftime("%d_%m_%Y")}"

    try:
        cur.execute(f"DROP TABLE {table_name}")
    except sqlite3.OperationalError:
        pass

    keys_string: str = ", ".join(keys)
    values_string: str = ", ".join("?" for _ in range(len(keys)))

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
