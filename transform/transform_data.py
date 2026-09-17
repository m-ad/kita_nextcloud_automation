import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class IncompleteSourceDataError(RuntimeError):
    """Signalisiert verworfene Quellzeilen nach einem sonst erfolgreichen Lauf.

    Wird bewusst erst *nach* dem Upload geworfen: die Stundenliste ist dann
    aktuell, der Exit-Code ungleich 0 macht die verworfenen Zeilen aber
    sichtbar, statt sie still zu überspringen.

    Parameters
    ----------
    defects:
        DataFrame mit den Spalten ``Eintrag`` und ``Grund``.
    """

    def __init__(self, defects: pd.DataFrame) -> None:
        self.defects = defects
        details = "\n".join(
            f"  - {row.Eintrag}: {row.Grund}" for row in defects.itertuples(index=False)
        )
        super().__init__(
            f"{len(defects)} unvollständige Zeile(n) in der Adressliste wurden "
            f"verworfen:\n{details}"
        )


def _is_blank(value) -> bool:
    """Prüfe, ob ein Zellwert leer ist (fehlend oder nur Leerzeichen)."""
    return pd.isna(value) or str(value).strip() == ""


def _defect_reasons(row: pd.Series, target_hours_dict: dict) -> list[str]:
    """Sammle die Gründe, warum eine Familie nicht ausgewertet werden kann.

    Parameters
    ----------
    row:
        Eine Zeile der aggregierten Familientabelle.
    target_hours_dict:
        Die hinterlegten SOLL-Stunden je (alleinerziehend, Anzahl Kinder).

    Returns
    -------
    list[str]
        Leere Liste, wenn die Familie vollständig ist.
    """
    reasons = []
    if _is_blank(row["Nextcloudaccount Mutter"]):
        reasons.append("Nextcloudaccount Mutter fehlt")
    if not row["alleinerziehend"] and _is_blank(row["Nextcloudaccount Vater"]):
        reasons.append("Nextcloudaccount Vater fehlt")
    if (row["alleinerziehend"], row["n_children"]) not in target_hours_dict:
        reasons.append(
            "keine SOLL-Stunden definiert für "
            f"(alleinerziehend={row['alleinerziehend']}, Kinder={row['n_children']})"
        )
    return reasons


def create_family_hours_table(
    df_hours: pd.DataFrame,
    df_names: pd.DataFrame,
    kita_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create a family hours table by merging hours data with names data.

    Unvollständig gepflegte Zeilen der Adressliste (fehlender Nachname der
    Mutter, fehlende Nextcloud-Accounts, unbekannte Kinderzahl) brechen die
    Auswertung nicht ab, sondern werden verworfen und protokolliert.

    Parameters
    ----------
    hours_df:
        DataFrame containing per-person work hours data.
    names_df:
        DataFrame containing names and adresses.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        Merged DataFrame with family hours and names sowie ein DataFrame der
        verworfenen Zeilen mit den Spalten ``Eintrag`` und ``Grund``.
    """
    # Define target hours per week based on age and single-parent status
    target_hours_dict = {
        (False, 1): 102,
        (False, 2): 132,
        (False, 3): 132,  # to be determined
        (True, 1): 50,
        (True, 2): 60,
    }

    defects: list[dict[str, str]] = []

    # filter for Kita year
    df_hours = df_hours.astype({"Datum": "datetime64[s]"}).query(
        f"'{kita_year}-09-15'<=Datum<'{kita_year + 1}-09-15'"
    )

    # Add family column
    df_names = df_names.assign(
        Familie=np.where(
            (df_names["Nachname Mutter"] == df_names["Nachname Vater"])
            | df_names["Nachname Vater"].isna()
            | df_names["Nachname Vater"].eq(""),
            df_names["Nachname Mutter"],
            df_names["Nachname Mutter"] + " & " + df_names["Nachname Vater"],
        )
    ).assign(
        alleinerziehend=lambda x: x["Nachname Vater"].isna()
        | x["Nachname Vater"].eq("")
    )
    # TODO: This ignores the case of single-parent fathers for now!

    # Zeilen ohne Familiennamen (Nachname Mutter noch nicht eingepflegt) lassen
    # sich keiner Familie zuordnen und müssen vor dem Gruppieren raus.
    without_family = df_names["Familie"].map(_is_blank)
    for _, row in df_names[without_family].iterrows():
        kind = " ".join(
            str(row[col])
            for col in ("Vorname Kind", "Nachname Kind")
            if not _is_blank(row[col])
        )
        defects.append(
            {
                # +1: der Index entspricht der (1-basierten) Zeile in Nextcloud
                "Eintrag": f"Zeile {int(row.name) + 1}: {kind or 'ohne Kindsnamen'}",
                "Grund": "Nachname Mutter fehlt, Familienname nicht ableitbar",
            }
        )
    df_names = df_names[~without_family]

    # create a dictionary Nextcloud ID -> total hours worked, e.g. {"m.meier": 37.5, "e.schmidt": 12.0}
    hours_dict: dict[str, float] = (
        df_hours.groupby("wer?_id")["Stunden"].sum().to_dict()
    )

    # create a dictionary family name -> children count, e.g. {"Musterfamilie": 2}
    children_count: dict[str, int] = (
        df_names.groupby("Familie")["Vorname Kind"].count().sort_values().to_dict()
    )

    # create family hours table
    family_hours = (
        df_names[
            [
                "Familie",
                "alleinerziehend",
                "Nextcloudaccount Mutter",
                "Nextcloudaccount Vater",
            ]
        ]
        .drop_duplicates(
            subset=["Familie"]
        )  # important! there is one row per child in df_names, so we need to drop duplicates to prevent double/triple counting
        .assign(
            stunden1=lambda x: x["Nextcloudaccount Mutter"].map(hours_dict).fillna(0)
        )
        .assign(
            stunden2=lambda x: x["Nextcloudaccount Vater"].map(hours_dict).fillna(0)
        )
        .assign(stunden_summe=lambda x: x["stunden1"] + x["stunden2"])
        .assign(n_children=lambda x: x["Familie"].map(children_count))
    )

    # Unvollständige Familien aussortieren, bevor die SOLL-Stunden zugeordnet
    # werden - sonst bricht der Lookup mit einem KeyError ab.
    complete = []
    for _, row in family_hours.iterrows():
        reasons = _defect_reasons(row, target_hours_dict)
        complete.append(not reasons)
        if reasons:
            defects.append(
                {"Eintrag": f"Familie {row['Familie']}", "Grund": "; ".join(reasons)}
            )
    family_hours = family_hours[np.array(complete, dtype=bool)]

    family_hours = (
        family_hours.assign(
            # keine Series.apply: die liefert bei leerem Input kein brauchbares Ergebnis
            target_hours=lambda x: [
                target_hours_dict[(alleinerziehend, n_children)]
                for alleinerziehend, n_children in zip(
                    x["alleinerziehend"], x["n_children"]
                )
            ]
        )
        .assign(
            progress=lambda x: np.round(
                np.minimum(100, x["stunden_summe"] / x["target_hours"] * 100)
            )
        )
        .astype({"progress": int})
        .sort_values(by="progress", ascending=False)
        .drop(
            columns=[
                "alleinerziehend",
                "n_children",
                "Nextcloudaccount Mutter",
                "Nextcloudaccount Vater",
            ]
        )
        .rename(
            columns={
                "target_hours": "Stunden SOLL",
                "stunden_summe": "Stunden IST",
                "progress": "Fortschritt",
                "stunden1": "Stunden Mutter",
                "stunden2": "Stunden Vater",
            },
            errors="ignore",
        )
    )

    for defect in defects:
        logger.warning(
            "Unvollständiger Eintrag verworfen - %s: %s",
            defect["Eintrag"],
            defect["Grund"],
        )

    return family_hours, pd.DataFrame(defects, columns=["Eintrag", "Grund"])
