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


def getCompletePackage(string: str, attributes: dict) -> dict:
    serviceSplit = string.split('\\')
    trackSplit = serviceSplit[0].split("/")

    return {
        "name": (serviceSplit[-1]).split(".")[0],
        "country": trackSplit[-2],
        "route": trackSplit[-1],
        "fahrplan": serviceSplit[-2],
        **attributes
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


def getTimesFromTimetableEntry(zug) -> (str, str):
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
        return "undefined", "undefined"

    try:
        duration = str(datetime.strptime(end, '%Y-%m-%d %H:%M:%S') - start_time)
    except ValueError:
        duration = "undefined"

    return datetime.strftime(start_time, "%H:%M"), duration


def getDataFromTimetables(timetables: list, config: Config):
    result = []

    for timetable in tqdm(timetables, desc="Durchsuche Fahrpläne nach Zugdiensten"):
        for service in [f.path for f in os.scandir(timetable) if config.datatype.service == f.path[-len(config.datatype.service):]]:
            if any([x.lower() in service.lower() for x in config.exclusionKeywords]):
                continue

            root = Et.parse(service).getroot()
            for type_tag in root.findall('Buchfahrplan'):
                # try to get service details
                try:
                    root_trn = Et.parse(f'{service[:-13]}trn').getroot()
                except FileNotFoundError:
                    continue

                zug = root_trn.findall('Zug')[0]
                start_time, duration = getTimesFromTimetableEntry(zug)
                fahrplanGruppe = zug.get('FahrplanGruppe')

                if any([x.lower() in fahrplanGruppe.lower() for x in config.exclusionKeywords]):
                    continue

                result.append(
                    getCompletePackage(
                        service,
                        {
                            "startzeit": start_time,
                            "fahrzeit": duration,
                            "gattung": zug.get('Gattung'),
                            "nummer": zug.get('Nummer'),
                            "zuglauf": zug.get('Zuglauf'),
                            "fahrplanGruppe": fahrplanGruppe,
                            "br": type_tag.get('BR')
                        }
                    )
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
    try:
        cur.execute("DROP TABLE zugdienste")
    except sqlite3.OperationalError:
        pass

    cur.execute("CREATE TABLE zugdienste(name, country, route, fahrplan, startzeit, fahrzeit, gattung, nummer, zuglauf, fahrplanGruppe, br)")
    cur.executemany("INSERT INTO zugdienste VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
    con.commit()

    print("Zugdienste in Datenbank eingetragen.")


def main():
    createDatabaseWithData(tuple(entry.values()) for entry in extrapolateDataFromZusi())


if __name__ == '__main__':
    main()
