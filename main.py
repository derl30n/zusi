import json
import os
import sqlite3
import xml.etree.ElementTree as Et
from dataclasses import dataclass, field
from datetime import datetime
from tqdm import tqdm


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


def getTimesFromTimetableEntry(zug, distance: int) -> (str, str):
    start: str = ""
    end: str = ""

    for trn_type_tag in zug.findall('FahrplanEintrag'):
        abf: str = trn_type_tag.get('Abf')

        if not abf:
            continue

        if not start:
            start = abf

        end = abf

    try:
        start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return "undefined", "undefined", 0

    duration = "undefined"
    dv = 0

    try:
        duration = datetime.strptime(end, '%Y-%m-%d %H:%M:%S') - start_time
        dv = int((distance / duration.seconds) * 3.6)
    except ValueError:
        pass
    except ZeroDivisionError:
        pass

    return datetime.strftime(start_time, "%H:%M"), str(duration), dv


def getPlannedStoppsFromTimetable(timetable) -> (list, int):
    stopps: list[str] = []
    rw: int = 0

    zeilen = timetable.findall('FplZeile')

    for zeile in zeilen:

        name = zeile.findall('FplName')
        if not name:
            continue

        ank = zeile.findall('FplAnk')
        if not ank:
            continue

        abf = zeile.findall('FplAbf')
        if not abf:
            continue

        stopps.append(name[0].get('FplNameText'))

        if zeile.find('FplRichtungswechsel') is not None:
            rw += 1

    return stopps, rw, int(float(zeilen[-1].get('FplLaufweg')))


def isServiceValid(service: str, flagged_words: list[str]) -> bool:
    return not any([x.lower() in service.lower() for x in flagged_words])


def getDataFromTimetables(timetables: list, config: Config):
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

                zug = root_trn.findall('Zug')[0]

                if not isServiceValid(zug.get('FahrplanGruppe'), config.exclusionKeywords):
                    continue

                planned_stops, n_turnarounds, planned_distance = getPlannedStoppsFromTimetable(type_tag)

                # TODO: re-work
                start_time, duration, dv = getTimesFromTimetableEntry(zug, planned_distance)

                result.append(
                    {
                        "gattung": type_tag.get('Gattung'),
                        "zugnr": type_tag.get('Nummer'),
                        "abfahrt": start_time,
                        "fahrzeit": duration,
                        "br": type_tag.get('BR'),
                        "laenge": int(float(type_tag.get('Laenge'))),
                        "masse": int(int(type_tag.get('Masse')) / 1000),
                        "nhalte": len(planned_stops),
                        "nwendungen": n_turnarounds,
                        "s_km": int(planned_distance / 1000),
                        "dv": dv,
                        **getServiceInfo(service),
                        "zuglauf": type_tag.get('Zuglauf'),
                        "halte": ", ".join(planned_stops)
                    }
                )

                break

    return result


def extrapolateDataFromZusi() -> list:
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


def createDatabaseWithData(data):
    con = sqlite3.connect("zugdienste.db")
    cur = con.cursor()

    table_name = f"_{datetime.now().strftime("%d_%m_%Y")}"

    try:
        cur.execute(f"DROP TABLE {table_name}")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        f"CREATE TABLE {table_name}(gattung, zugnr, abfahrt, fahrzeit, br, laenge, masse, nhalte, nwendungen, s_km, dv, country, route, fahrplan, zuglauf, halte)")
    cur.executemany(f"INSERT INTO {table_name} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
    con.commit()

    print("Zugdienste in Datenbank eingetragen.")


def main():
    createDatabaseWithData(tuple(entry.values()) for entry in extrapolateDataFromZusi())


if __name__ == '__main__':
    main()
