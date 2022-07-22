import csv
from collections import defaultdict
from pathlib import Path
from typing import List, Union, Dict, Tuple
import pandas as pd
from datetime import datetime

csv.register_dialect('csvrd', delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)

CSV_ZAEHLERNUMMERN = (Path('./data', 'zaehlernummern.csv'),
                      ["bezeichnung", "zaehlernummer", "notizen"])
CSV_ZAEHLERSTAENDE = (Path('./data', 'zaehlerstaende.csv'),
                      ["datum", "zaehlernummer", "zaehlerstand", "notizen"])
CSV_STROMPREISE = (Path('./data', 'strompreise.csv'),
                   ["datum", "zaehlernummer", "strompreis_nett", "grundpreis_netto", "notizen"])
CSV_RECHNUNGSDATEN = (Path('./data', 'rechnungsdaten.csv'),
                      ["bezeichnung", "rechnungsdatum", "notizen"])
ALL_CSV = [CSV_ZAEHLERNUMMERN, CSV_ZAEHLERSTAENDE, CSV_STROMPREISE, CSV_RECHNUNGSDATEN]


def flatten(l) -> list:
    return [i for j in l for i in j]


def init_csv():
    for csv_data in ALL_CSV:
        if not csv_data[0].exists():
            csv_data[0].parent.mkdir(exist_ok=True, parents=True)
            with open(csv_data[0], encoding='utf-8', mode='w') as csv_file:
                csv_writer = csv.writer(csv_file, quotechar='"', delimiter=';', quoting=csv.QUOTE_ALL)
                csv_writer.writerow(csv_data[1])


def get_zaehlernummern() -> Dict[str, Union[List[str], str]]:
    reader = csv.reader(CSV_ZAEHLERNUMMERN[0].read_text().splitlines(True), 'csvrd')
    bezeichnung, zaehlernummer, _ = zip(*reader)
    d = defaultdict(list)
    [d[bez].append(zaehlernummer[index + 1]) for index, bez in enumerate(bezeichnung[1:])]
    return dict(d)


def get_zaehlerstaende(zaehlernummern: Dict[str, List[str]]):
    reader = csv.reader(CSV_ZAEHLERSTAENDE[0].read_text().splitlines(True), 'csvrd')
    datum, zaehlernr, zaehlerstand, _ = zip(*reader)
    repl_dict = {v: k for k, val in zaehlernummern.items() for v in flatten(zaehlernummern.values()) if v in val}
    d = defaultdict(list)
    [d[repl_dict[zn]].append((dat, zn, zs)) for dat, zn, zs in zip(datum[1:], zaehlernr[1:], zaehlerstand[1:])]
    return dict(d)


def get_rechnungsdaten():
    reader = csv.reader(CSV_RECHNUNGSDATEN[0].read_text().splitlines(True), 'csvrd')
    bezeichnung, rechnungsdatum, _ = zip(*reader)
    return {k: v for k, v in zip(bezeichnung[1:], rechnungsdatum[1:])}


def as_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, '%Y-%m-%d')


def get_strompreise(zaehlernummern: Dict[str, List[str]]) -> Dict[str, List[Tuple[str, str, str, str, str]]]:
    reader = csv.reader(CSV_STROMPREISE[0].read_text().splitlines(True), 'csvrd')
    datum, zaehlernr, arbeitspreis, grundpreis, mwst, _ = zip(*reader)
    repl_dict = {v: k for k, val in zaehlernummern.items() for v in flatten(zaehlernummern.values()) if v in val}
    d = defaultdict(list)
    [d[repl_dict[zn]].append((as_datetime(dat), zn, ap, gp, mw)) for
     dat, zn, ap, gp, mw in zip(datum[1:], zaehlernr[1:], arbeitspreis[1:], grundpreis[1:], mwst[1:])]
    return dict(d)


def main():
    init_csv()
    zaehlernummern = get_zaehlernummern()
    strompreise = get_strompreise(zaehlernummern)
    rechnungsdaten = get_rechnungsdaten()
    zaehlerstaende = get_zaehlerstaende(zaehlernummern)
    pass


if __name__ == '__main__':
    main()
