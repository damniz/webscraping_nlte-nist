import os.path

import requests
from bs4 import BeautifulSoup
from itertools import product
import pandas as pd
from datetime import datetime


def build_url(
    base_url: str,
    nuc_charge: int,
    rho: int,
    temperature: float,
    energy_no_1: float,
    energy_no_2: float,
    opacities: list,
):
    url = (
        f"{base_url}?nuc_charge={nuc_charge}&rho={rho}&temperature={temperature}"
        f"&energy_no_1={energy_no_1}&energy_no_2={energy_no_2}"
    )
    for opacity in opacities:
        url += f"&opacity={opacity}"
    return url


if __name__ == "__main__":
    start_time = datetime.now()
    max_retries = 3
    base_url = "https://nlte.nist.gov/cgi-bin/OPAC/osearch.py"
    nuc_charges = list(range(57, 71)) + list(range(89, 103))
    rhos = list(range(4, 21))
    temperatures = [
        ".01",
        0.07,
        0.1,
        0.14,
        0.17,
        0.2,
        0.22,
        0.24,
        0.27,
        0.3,
        0.34,
        0.4,
        0.5,
        0.6,
        0.7,
        0.8,
        0.9,
        1,
        1.2,
        1.5,
        2,
        2.5,
        3,
        3.5,
        4,
        4.5,
        5,
    ]
    energy_no_1 = 1.25e-5
    energy_no_2 = 1.5e5
    opacities = ["total_opac", "absorpt_opac", "bb_opac", "bf_opac", "ff_opac"]
    if os.path.isfile("lanthanide_actinide-opacity-database.pkl"):
        full_df = pd.read_pickle("lanthanide_actinide-opacity-database.pkl")
    else:
        full_df = pd.DataFrame()
    total_combinations = len(nuc_charges) * len(rhos) * len(temperatures)
    error_index = []
    for i, (nuc_charge, rho, temperature) in enumerate(
        product(nuc_charges, rhos, temperatures)
    ):
        full_df.sort_index(inplace=True)
        if (nuc_charge, 10 ** (-rho), float(temperature)) not in full_df.index:
            retries = 1
            proceed = True
            while retries <= max_retries and proceed:
                print(
                    f"\n{datetime.now()} : Downloading dataframe {i + 1} / {total_combinations}"
                    f" ({100 * (i + 1) / total_combinations:.2f}% achieved)"
                )
                if i > 0:
                    time_remaining = (
                        (datetime.now() - start_time) * (total_combinations - i) / i
                    )
                    print(
                        f"{datetime.now()} : Expected remaining time : {time_remaining}"
                    )
                print(
                    f"{datetime.now()} : Nuclear charge : {nuc_charge}\n"
                    f"{datetime.now()} : Mass density : {10 ** (-rho)} g/cm\u00b3\n"
                    f"{datetime.now()} : Electron temperature : {temperature} eV"
                )
                url = build_url(
                    base_url,
                    nuc_charge,
                    rho,
                    temperature,
                    energy_no_1,
                    energy_no_2,
                    opacities,
                )
                try:
                    response = requests.get(url, timeout=15)
                    soup = BeautifulSoup(response.content, "html.parser")
                    table = soup.find("table")
                    headers = [header.text for header in table.find_all("th")]
                    rows = []
                    for row in table.find_all("tr")[1:]:
                        cells = row.find_all("td")
                        rows.append([cell.text for cell in cells])
                    df = pd.DataFrame(rows, columns=headers)
                    df["nuclear_charge"] = nuc_charge
                    df["mass_density_rho(g/cm3)"] = 10 ** (-rho)
                    df["electron_temperature(eV)"] = temperature
                    df = df.map(pd.to_numeric)
                    df.set_index(
                        [
                            "nuclear_charge",
                            "mass_density_rho(g/cm3)",
                            "electron_temperature(eV)",
                        ],
                        inplace=True,
                    )
                    full_df = pd.concat([full_df, df])
                    full_df.to_pickle("lanthanide_actinide-opacity-database.pkl")
                    proceed = False
                    print(f"{datetime.now()} : Dataframe saved")
                except Exception as e:
                    print(f"{datetime.now()} : Server error {e} !")
                    retries += 1
    full_df.to_csv("lanthanide_actinide-opacity-database.csv")
