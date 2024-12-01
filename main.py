import json
import os
import sqlite3
import xml.etree.ElementTree as Et
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from tqdm import tqdm


@dataclass(frozen=True)
class ServiceEntry:
    name: str = field(compare=True)
    timeArr: datetime | None = field(compare=False)
    timeDep: datetime | None = field(compare=False)
    isTurnAround: bool = field(compare=False)
    isPlannedStop: bool = field(compare=False)
    runningDistance: int = field(compare=False)
    # isValid: bool = field(compare=False)

    # def __post_init__(self) -> None:
    #     self.isValid = all([self.timeDep, self.runningDistance])

    def isValid(self) -> bool:
        return all([self.timeDep, self.runningDistance])

    def isAtPlatform(self) -> bool:
        return self.timeArr is not None


class ServiceData:
    def __init__(self, schedule) -> None:
        self.start: ServiceEntry = ServiceEntry("", None, None, False, False, 0)
        self.end: ServiceEntry = ServiceEntry("", None, None, False, False, 0)
        self.plannedStopps: list = []
        self.turnarounds: int = 0
        self.isValid: bool = False

        self._constructService(schedule)

    def _constructService(self, schedule) -> None:
        entries = schedule.findall('FplZeile')

        if not len(entries) > 0:
            return

        for entry in entries:
            scheduleEntry = self._getScheduleEntryFromEntry(entry)

            if not scheduleEntry.isValid():
                continue

            self.end = scheduleEntry

            if not self.start.isValid():
                self.start = scheduleEntry

            if scheduleEntry.isPlannedStop:
                isPlannedStoppsPopulated = len(self.plannedStopps) > 0

                # Do not add the entry point as a planned stop
                if not isPlannedStoppsPopulated and scheduleEntry == self.start:
                    continue

                # Do not add the previous stopp again
                if isPlannedStoppsPopulated and scheduleEntry == self.plannedStopps[-1]:
                    continue

                self.plannedStopps.append(scheduleEntry)

            # TODO: does a turnaround also need to be planned stop?
            if scheduleEntry.isTurnAround:
                self.turnarounds += 1

        self.isValid = self.start.isValid() and self.end.isValid()

    def _getScheduleEntryFromEntry(self, entry) -> ServiceEntry:
        name = entry.findall('FplName')
        arr = entry.findall('FplAnk')
        dep = entry.findall('FplAbf')
        isTA = entry.find('FplRichtungswechsel') is not None
        isPS = all([name, arr, dep])
        dist = entry.get('FplLaufweg')

        nameStr = name[0].get('FplNameText') if len(name) > 0 else None
        arrStr = self.getTime(arr[0].get('Ank')) if len(arr) > 0 else None
        depStr = self.getTime(dep[0].get('Abf')) if len(dep) > 0 else None
        distInt = int(float(dist)) if dist is not None else 0

        return ServiceEntry(
            nameStr,
            arrStr,
            depStr,
            isTA,
            isPS,
            distInt
        )

    @staticmethod
    def getTime(timeString: str | None) -> datetime | None:
        if timeString is None:
            return None

        try:
            return datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None

    def getPlannedStopNamesAsList(self) -> list[str]:
        return [entry.name for entry in self.plannedStopps]

    def getStartTimeFormatted(self) -> str:
        return datetime.strftime(self.start.timeDep, "%H:%M")

    def getDuration(self) -> timedelta:
        return self.end.timeDep - self.start.timeDep

    def getAvgSpeed(self) -> int:
        duration = self.getDuration().seconds

        if duration == 0:
            return 0

        return int((self.end.runningDistance / duration) * 3.6)


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


def getServiceInfo(string: str) -> dict:
    serviceSplit = string.split('\\')
    trackSplit = serviceSplit[0].split("/")

    return {
        "country": trackSplit[-2],
        "route": trackSplit[-1],
        "fahrplan": serviceSplit[-2]
    }


def getTimetablesFromZusiFiles(config: Config) -> list:
    timetables = []

    try:
        for cfgPath in config.paths:
            for country in os.listdir(cfgPath):  # looping countries
                if country.lower() in config.exclusionKeywords:
                    continue

                for route in os.listdir(f'{cfgPath}/{country}'):  # looping routes
                    timetables.extend(
                        [f.path[:-(len(config.datatype.timetable) + 1)] for f in os.scandir(f'{cfgPath}/{country}/{route}')
                         if config.datatype.timetable == f.path[-len(config.datatype.timetable):]]
                    )
    except FileNotFoundError as e:
        print(e)

    return timetables


def isServiceValid(service: str, flagged_words: list[str]) -> bool:
    return not any([x.lower() in service.lower() for x in flagged_words])


def getDataFromTimetables(timetables: list, config: Config) -> list[dict]:
    result = []

    for timetable in tqdm(timetables, desc="Durchsuche Fahrpläne nach Zugdiensten"):
        for service in [f.path for f in os.scandir(timetable) if config.datatype.service == f.path[-len(config.datatype.service):]]:
            if not isServiceValid(service, config.exclusionKeywords):
                continue

            root = Et.parse(service).getroot()
            for type_tag in root.findall('Buchfahrplan'):
                # try to get service details
                try:
                    root_trn = Et.parse(f'{service[:-13]}trn').getroot()
                except FileNotFoundError:
                    continue

                if not isServiceValid(root_trn.findall('Zug')[0].get('FahrplanGruppe'), config.exclusionKeywords):
                    continue

                serviceData = ServiceData(type_tag)

                if not serviceData.isValid:
                    print("service not valid!!", service)
                    continue

                planned_stops = serviceData.getPlannedStopNamesAsList()
                n_turnarounds = serviceData.turnarounds
                planned_distance = serviceData.end.runningDistance

                start_time = serviceData.getStartTimeFormatted()
                duration = str(serviceData.getDuration())
                dv = serviceData.getAvgSpeed()
                agp = serviceData.start.name

                result.append(
                    {
                        "gattung": type_tag.get('Gattung'),
                        "zugnr": type_tag.get('Nummer'),
                        "begin": start_time,
                        "fahrzeit": duration,
                        "br": type_tag.get('BR'),
                        "laenge": int(float(type_tag.get('Laenge'))),
                        "masse": int(int(type_tag.get('Masse')) / 1000),
                        "nhalte": len(planned_stops),
                        "w1": n_turnarounds,
                        "v3": serviceData.start.isAtPlatform(),
                        "a3": serviceData.end.isAtPlatform(),
                        "s_km": int(planned_distance / 1000),
                        "dv": dv,
                        **getServiceInfo(service),
                        "aufgleispunkt": agp,
                        "zuglauf": type_tag.get('Zuglauf'),
                        "halte": ", ".join(planned_stops)
                    }
                )

                break

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
