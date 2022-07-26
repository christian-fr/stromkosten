import matplotlib.pyplot as plt
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import List, Union, Dict, Tuple
from datetime import datetime, timedelta

csv.register_dialect('csvrd', delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)

CSV_ZAEHLERNUMMERN = (Path('./data', 'zaehlernummern.csv'),
                      ["bezeichnung", "zaehlernummer", "offset", "notizen"])
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
    bezeichnung, zaehlernummer, offset, _ = zip(*reader)
    d = defaultdict(list)
    [d[bez].append(zaehlernummer[1:][index]) for index, bez in enumerate(bezeichnung[1:])]

    o = {zn: float(off) if off.strip() != '' else float(0) for zn, off in zip(zaehlernummer[1:], offset[1:])}
    return dict(d), dict(o)


def get_zaehlerstaende(zaehlernummern: Dict[str, List[str]], offsets: Dict[str, float]) -> Dict[
    str, List[Tuple[datetime, str, float]]]:
    reader = csv.reader(CSV_ZAEHLERSTAENDE[0].read_text().splitlines(True), 'csvrd')
    datum, zaehlernr, zaehlerstand, _ = zip(*reader)
    repl_dict = {v: k for k, val in zaehlernummern.items() for v in flatten(zaehlernummern.values()) if v in val}
    d = defaultdict(list)
    [d[repl_dict[zn]].append((as_datetime(dat), zn, float(zs) + offsets[zn])) for dat, zn, zs in
     zip(datum[1:], zaehlernr[1:], zaehlerstand[1:])]
    return dict(d)


def add_usage(zs_list: List[Tuple[datetime, str, float]]
              ) -> List[Tuple[datetime, str, float, float]]:
    output_list = []
    for index, entry in enumerate(zs_list):
        ts, zn, meas = entry
        if index >= len(zs_list) - 1:
            output_list.append((ts, zn, meas, None))
            break
        next_ts, next_zn, next_meas = zs_list[index + 1]
        date_diff = (next_ts - ts).days
        usage_diff = next_meas - meas
        if date_diff == 0:
            continue
        else:
            usage_mean = usage_diff / date_diff
        output_list.append((ts, zn, meas, usage_mean))
    return output_list


def get_rechnungsdaten() -> Dict[str, datetime]:
    reader = csv.reader(CSV_RECHNUNGSDATEN[0].read_text().splitlines(True), 'csvrd')
    bezeichnung, rechnungsdatum, _ = zip(*reader)
    return {k: get_next_invoice_date(as_datetime(v)) for k, v in zip(bezeichnung[1:], rechnungsdatum[1:])}


def as_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, '%Y-%m-%d')


def get_strompreise(zaehlernummern: Dict[str, List[str]]) -> Dict[
    str, List[Tuple[datetime, datetime, str, float, float, float]]]:
    reader = csv.reader(CSV_STROMPREISE[0].read_text().splitlines(True), 'csvrd')
    datum, zaehlernr, arbeitspreis, grundpreis, mwst, _ = zip(*reader)
    repl_dict = {v: k for k, val in zaehlernummern.items() for v in flatten(zaehlernummern.values()) if v in val}
    d = defaultdict(list)

    [d[repl_dict[zn]].append((as_datetime(dat), zn, float(ap), float(gp), float(mw))) for
     dat, zn, ap, gp, mw in zip(datum[1:], zaehlernr[1:], arbeitspreis[1:], grundpreis[1:], mwst[1:])]

    for data in d.values():
        data.sort(key=lambda k: k[0])
        for index, entry in enumerate(data):
            if index < len(data) - 1:
                data[index] = (
                    entry[0], data[index + 1][0] + timedelta(days=-1), entry[1], entry[2], entry[3], entry[4])
            elif index == len(data) - 1:
                data[index] = (entry[0], datetime(year=3000, month=12, day=31), entry[1], entry[2], entry[3], entry[4])
    return dict(d)


#
# def calc_usage_whole_year(start_date: datetime,
#                           end_date: datetime,
#                           zaehlerstaende_list: List[Tuple[datetime, str, float]]) -> float:
#     zaehlerstaende_list.sort(key=lambda k: k[0])
#
#     relevant_meas_indices = [index for index, data in enumerate(zaehlerstaende_list)
#                              if start_date <= data[0] <= end_date]
#     min_index, max_index = min(relevant_meas_indices), max(relevant_meas_indices)
#     if min_index > 0:
#         relevant_meas_indices = [min_index - 1] + relevant_meas_indices
#     if max_index < len(zaehlerstaende_list) - 2:
#         relevant_meas_indices.append(max_index + 1)
#     relevant_meas = zaehlerstaende_list[min(relevant_meas_indices):max(relevant_meas_indices) + 1]
#
#     usage_dict = {}
#     if start_date in [ts for ts, _, _ in zaehlerstaende_list]:
#         pass
#
#     for day in [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]:
#         last_ts, last_zs, last_usage = None, None, None
#         for index, data in enumerate(zaehlerstaende_list):
#             ts, _, zs = data
#             if last_ts == day:
#                 pass
#             elif last_ts is None:
#                 last_ts = ts
#                 last_zs = zs
#                 last_usage = None
#                 continue
#             if index >= len(zaehlerstaende_list) - 1:
#                 pass


def get_next_invoice_date(invoice_date: datetime) -> datetime:
    now = datetime.now()
    recent_invoice_date = datetime(year=now.year, month=invoice_date.month, day=invoice_date.day)
    if now < recent_invoice_date:
        return recent_invoice_date
    else:
        return datetime(year=recent_invoice_date.year + 1,
                        month=invoice_date.month,
                        day=invoice_date.day)


def monthrange(input_date: datetime) -> int:
    tmp_date = datetime(year=input_date.year, month=input_date.month, day=1)
    return (add_months(tmp_date, 1) - tmp_date).days


def calc_days_list(start_date: datetime, end_date: datetime) -> List[datetime]:
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]


def calc_prices_for_period(start_date: datetime, end_date: datetime,
                           strompreise_list: List[Tuple[datetime, datetime, str, float, float, float]]
                           ) -> Tuple[float, float]:
    days_list = calc_days_list(start_date=start_date, end_date=end_date)
    invoice_range_days = len(days_list)

    ap_days_dict = {}
    for date in days_list:
        for entry in strompreise_list:
            if entry[0] <= date <= entry[1]:
                ap_days_dict[date] = (entry[3], entry[5])
                break
    gp_days_dict = {}
    for date in days_list:
        for entry in strompreise_list:
            if entry[0] <= date <= entry[1]:
                gp_days_dict[date] = (entry[4] / monthrange(date), entry[5])
                break

    a = sum([ap * (1 + mw * 0.01) for ap, mw in ap_days_dict.values()]) / invoice_range_days
    b = sum([gp * (1 + mw * 0.01) for gp, mw in gp_days_dict.values()]) / invoice_range_days
    return (sum([ap * (1 + mw * 0.01) for ap, mw in ap_days_dict.values()]) / invoice_range_days,
            sum([gp * (1 + mw * 0.01) for gp, mw in gp_days_dict.values()]) / invoice_range_days)


def add_months(date: datetime, months: int) -> datetime:
    if months == 0:
        return date
    year = date.year
    month = date.month
    day = date.day
    hour = date.hour
    min = date.minute
    sec = date.second

    if months < 0:
        year = year + math.ceil(months / 12)
        months = months % -12

    if months > 0:
        year = year + math.floor(months / 12)
        months = months % 12

    if month + months <= 0:
        year = year - 1
        month = 12 + month + months
    elif month + months > 12:
        year = year + 1
        month = month + months - 12
    else:
        month = month + months
    try:
        return_date = datetime(year=year, month=month, day=day, hour=hour, minute=min, second=sec)
    except ValueError as err:
        print(err)
        if day == 29:
            day = 28
        return_date = datetime(year=year, month=month, day=day, hour=hour, minute=min, second=sec)
    return return_date


def calc_usage_period(zaehlerstaende: List[Tuple[datetime, str, float, float]],
                      rechnungsdatum: datetime) -> Dict[datetime, Tuple[float, float, bool]]:
    days_list = calc_days_list(add_months(rechnungsdatum, -12), rechnungsdatum)
    last_index = 0
    usage_dict = {}
    flag_estimate = False
    all_usage_means = []
    mean_all_usage_means = None
    for day in days_list:
        for index, data in enumerate(zaehlerstaende[last_index:]):
            if index + last_index > len(zaehlerstaende) - 2:
                flag_estimate = True
                break
            ts, _, meas, mean = data
            next_ts, _, _, _ = zaehlerstaende[index + 1 + last_index]
            if date_between_dates(day, ts, next_ts):
                all_usage_means.append(mean)
                if day in [d[0] for d in zaehlerstaende]:
                    usage_dict[day] = (meas, mean, True)
                else:
                    usage_dict[day] = (meas + (day - ts).days * mean, mean, True)
                last_index = index + last_index
                break

        if day not in usage_dict.keys():
            if mean_all_usage_means is None:
                mean_all_usage_means = statistics.mean(all_usage_means)
            usage_dict[day] = (
                usage_dict[day + timedelta(days=-1)][0] + mean_all_usage_means, mean_all_usage_means, False)
    return usage_dict


def calc_invoice_value_next_year(strom_prices_next_year: Tuple[float, float],
                                 usage_next_year: Dict[datetime, Tuple[float, float, bool]]):
    diff = usage_next_year[max(usage_next_year.keys())][0] - usage_next_year[min(usage_next_year.keys())][0]
    return (diff, diff * strom_prices_next_year[0] / 100, len(usage_next_year.keys()) * strom_prices_next_year[1])


def date_between_dates(date: datetime, date1: datetime, date2: datetime):
    if date1 == date2:
        return date == date1
    elif date1 < date2:
        return date1 <= date < date2
    else:
        return date2 <= date < date1


def main():
    init_csv()
    zaehlernummern, offsets = get_zaehlernummern()
    strompreise = get_strompreise(zaehlernummern=zaehlernummern)
    rechnungsdaten = get_rechnungsdaten()
    zaehlerstaende = get_zaehlerstaende(zaehlernummern=zaehlernummern, offsets=offsets)
    zaehlerstaende_mod = {bez: add_usage(zs_data) for bez, zs_data in zaehlerstaende.items()}

    # set start and end timestamps
    period_start1 = {bez: add_months(rechnungsdatum, -12) for bez, rechnungsdatum in rechnungsdaten.items()}
    period_end1 = {bez: rechnungsdatum for bez, rechnungsdatum in rechnungsdaten.items()}
    period_start2 = {bez: add_months(rechnungsdatum, -24) for bez, rechnungsdatum in rechnungsdaten.items()}
    period_end2 = {bez: add_months(rechnungsdatum, -12) for bez, rechnungsdatum in rechnungsdaten.items()}

    period_start3 = {bez: add_months(rechnungsdatum, -36) for bez, rechnungsdatum in rechnungsdaten.items()}
    period_end3 = {bez: add_months(rechnungsdatum, -24) for bez, rechnungsdatum in rechnungsdaten.items()}

    usage_period1, invoice_value_period1 = estimate_usage_period(period_start1, period_end1,
                                                                 zaehlerstaende_mod=zaehlerstaende_mod,
                                                                 strompreise=strompreise)
    usage_period2, invoice_value_period2 = estimate_usage_period(period_start2, period_end2,
                                                                 zaehlerstaende_mod=zaehlerstaende_mod,
                                                                 strompreise=strompreise)
    usage_period3, invoice_value_period3 = estimate_usage_period(period_start3, period_end3,
                                                                 zaehlerstaende_mod=zaehlerstaende_mod,
                                                                 strompreise=strompreise)

    f = {bez: [entry[2] for entry in usage_list.values()].count(False) / len(
        [entry[2] for entry in usage_list.values()])
         for bez, usage_list in usage_period1.items()}

    print("estimation ratio (% of days estimated)")
    print(f)
    print("Verbauch kWh, EUR Arbeitspreis, EUR Grundpreis")
    print(invoice_value_period1)
    print("\nestimated sums")
    print(f'{round(sum([val[1] for val in invoice_value_period1.values()]),2)} EUR')
    print(f'{round(sum([val[0] for val in invoice_value_period1.values()]))} kWh')

    fig, ax = plt.subplots()
    ax.set_title('Stromverbrauch - Prognose')
    xv1, yv1, tv1 = prepare_values_usage_period(usage_period1)
    xv2, yv2, tv2 = prepare_values_usage_period(usage_period2)
    xv3, yv3, tv3 = prepare_values_usage_period(usage_period3)
    label_true1 = f'{xv1[0].strftime("%Y-%m-%d")} to {xv1[-1].strftime("%Y-%m-%d")} '
    label_false1 = f'{xv1[0].strftime("%Y-%m-%d")} to {xv1[-1].strftime("%Y-%m-%d")} estimated'
    plot_usage_values(xv1, yv1, tv1, 'blue', 'violet', label_true1, label_false1, 3, 2)

    year_diff2 = xv1[0].year - xv2[0].year
    label_true2 = f'{xv2[0].strftime("%Y")} to {xv2[-1].strftime("%Y")} '
    label_false2 = f'{xv2[0].strftime("%Y")} to {xv2[-1].strftime("%Y")} estimated'
    xv2_mod = [add_months(x, 12 * year_diff2) for x in xv2]
    plot_usage_values(xv2_mod, yv2, tv2, 'pink', 'pink', label_true2, label_false2)

    year_diff3 = xv1[0].year - xv3[0].year
    label_true3 = f'{xv3[0].strftime("%Y")} to {xv3[-1].strftime("%Y")} '
    label_false3 = f'{xv3[0].strftime("%Y")} to {xv3[-1].strftime("%Y")} estimated'
    xv3_mod = [add_months(x, 12 * year_diff3) for x in xv3]
    plot_usage_values(xv3_mod, yv3, tv3, 'lightgrey', 'lightgrey', label_true3, label_false3)

    ax.legend(loc="upper left")
    ax.set_xlabel('')
    ax.set_ylabel('kWh kumuliert')
    ax.legend(markerscale=10)

    fig.show()


def plot_usage_values(x, y, t, color_true, color_false, label_true, label_false, s_true=1, s_false=1):
    if False in t:
        plt.scatter(x=[xv[0] for xv in zip(x, t) if not xv[1]], y=[yv[0] for yv in zip(y, t) if not yv[1]],
                    color=color_false, marker='.', s=s_false, label=label_false)
    plt.scatter(x=[xv[0] for xv in zip(x, t) if xv[1]], y=[yv[0] for yv in zip(y, t) if yv[1]], color=color_true,
                marker='.', s=s_true, label=label_true)


def estimate_usage_period(period_start: Dict[str, datetime], period_end: Dict[str, datetime],
                          zaehlerstaende_mod: Dict[str, List[Tuple[datetime, str, float, float]]],
                          strompreise: Dict[str, List[Tuple[datetime, datetime, str, float, float, float]]]):
    usage_period = {bez: calc_usage_period(zaehlerstaende=zaehlerstaende_list,
                                           rechnungsdatum=period_end[bez])
                    for bez, zaehlerstaende_list in zaehlerstaende_mod.items()}
    electr_prices_period = {bez: calc_prices_for_period(start_date=period_start[bez],
                                                        end_date=period_end[bez],
                                                        strompreise_list=strompreise[bez])
                            for bez, strompreis_data in strompreise.items()}
    invoice_value_period = {bez: calc_invoice_value_next_year(electr_prices_period[bez], usage_period[bez])
                            for bez, usage_data in usage_period.items()}

    return usage_period, invoice_value_period


def prepare_values_usage_period(usage_period):
    usage_next_year_cum = {}
    for val in usage_period.values():
        for day, data in val.items():
            if day in usage_next_year_cum.keys():
                usage_next_year_cum[day] = (
                    usage_next_year_cum[day][0] + data[0], usage_next_year_cum[day][1] and data[2])
            else:
                usage_next_year_cum[day] = (data[0], data[2])
    usage_next_year_cum_sum = max([d[0] for d in usage_next_year_cum.values()]) - min(
        [d[0] for d in usage_next_year_cum.values()])
    pass
    # usage_next_year_cum[day] += meas
    xv, yv, tv = zip(*[(x, y[0], y[1]) for x, y in usage_next_year_cum.items()])

    min_y = min(yv)
    yv = [v - min_y for v in yv]

    return xv, yv, tv


if __name__ == '__main__':
    main()
    exit()