"""Tests für das Verwerfen unvollständiger Zeilen in der Stundenliste.

Alle Namen in diesen Tests sind frei erfunden - Echtdaten aus den
Nextcloud-Tabellen gehören nicht ins Repo.
"""

import pandas as pd

from transform.transform_data import create_family_hours_table

KITA_YEAR = 2026

NAMES_COLUMNS = [
    "Vorname Kind",
    "Nachname Kind",
    "Nextcloudaccount Mutter",
    "Nextcloudaccount Vater",
    "Nachname Mutter",
    "Nachname Vater",
]


def make_names(*rows: dict) -> pd.DataFrame:
    """Baue eine Adressliste (eine Zeile je Kind) mit allen benötigten Spalten."""
    return pd.DataFrame(list(rows), columns=NAMES_COLUMNS)


def make_hours(*rows: dict) -> pd.DataFrame:
    """Baue eine Stundenliste im Format der exploded Nextcloud-Tabelle."""
    return pd.DataFrame(list(rows), columns=["Datum", "Stunden", "wer?_id"])


def child(vorname: str, familie: str, **overrides) -> dict:
    """Eine vollständige Adresslisten-Zeile für ein Kind mit zwei Elternteilen."""
    row = {
        "Vorname Kind": vorname,
        "Nachname Kind": familie,
        "Nextcloudaccount Mutter": f"m.{familie.lower()}",
        "Nextcloudaccount Vater": f"v.{familie.lower()}",
        "Nachname Mutter": familie,
        "Nachname Vater": familie,
    }
    row.update(overrides)
    return row


def booking(account: str, datum: str, stunden: float) -> dict:
    """Eine Buchung der Stundenliste."""
    return {"Datum": datum, "Stunden": stunden, "wer?_id": account}


def test_complete_data_yields_expected_table():
    names = make_names(child("Alice", "Alpha"), child("Bob", "Alpha"))
    hours = make_hours(
        booking("m.alpha", "2026-10-01", 40.0),
        booking("v.alpha", "2026-11-02", 26.0),
    )

    table, defects = create_family_hours_table(hours, names, KITA_YEAR)

    assert defects.empty
    row = table.iloc[0]
    assert row["Familie"] == "Alpha"
    assert row["Stunden IST"] == 66.0
    assert row["Stunden SOLL"] == 132  # zwei Kinder, nicht alleinerziehend
    assert row["Fortschritt"] == 50


def test_double_family_name_and_single_parent():
    names = make_names(
        child("Bob", "Beta", **{"Nachname Mutter": "Gamma"}),
        child("Carol", "Delta", **{"Nachname Vater": None}),
    )
    hours = make_hours(booking("v.delta", "2026-09-15", 25.0))

    table, defects = create_family_hours_table(hours, names, KITA_YEAR)

    assert defects.empty
    table = table.set_index("Familie")
    assert set(table.index) == {"Gamma & Beta", "Delta"}
    # Alleinerziehende mit einem Kind: 50 SOLL-Stunden
    assert table.loc["Delta", "Stunden SOLL"] == 50
    assert table.loc["Delta", "Fortschritt"] == 50


def test_missing_nextcloud_account_drops_family_and_is_reported():
    names = make_names(
        child("Alice", "Alpha"),
        child("Bob", "Beta", **{"Nextcloudaccount Vater": ""}),
    )
    hours = make_hours(booking("m.alpha", "2026-10-01", 10.0))

    table, defects = create_family_hours_table(hours, names, KITA_YEAR)

    assert list(table["Familie"]) == ["Alpha"]
    assert len(defects) == 1
    assert defects.iloc[0]["Eintrag"] == "Familie Beta"
    assert "Nextcloudaccount Vater fehlt" in defects.iloc[0]["Grund"]


def test_missing_mother_surname_drops_row():
    names = make_names(
        child("Alice", "Alpha"),
        child("Bob", "Beta", **{"Nachname Mutter": None}),
    )

    table, defects = create_family_hours_table(make_hours(), names, KITA_YEAR)

    assert list(table["Familie"]) == ["Alpha"]
    assert len(defects) == 1
    assert "Nachname Mutter fehlt" in defects.iloc[0]["Grund"]
    # Zeilennummer 1-basiert wie in der Nextcloud-Tabelle
    assert defects.iloc[0]["Eintrag"] == "Zeile 2: Bob Beta"


def test_unknown_target_hours_combination_does_not_raise():
    """Regression: (alleinerziehend=True, 3 Kinder) hat keine SOLL-Stunden."""
    names = make_names(
        *(
            child(vorname, "Delta", **{"Nachname Vater": None})
            for vorname in ("Carol", "Dave", "Erin")
        )
    )

    table, defects = create_family_hours_table(make_hours(), names, KITA_YEAR)

    assert table.empty
    assert len(defects) == 1
    assert "keine SOLL-Stunden definiert" in defects.iloc[0]["Grund"]


def test_hours_outside_kita_year_are_ignored():
    names = make_names(child("Alice", "Alpha"))
    hours = make_hours(
        booking("m.alpha", "2026-09-14", 5.0),  # letzter Tag des Vorjahres
        booking("m.alpha", "2027-09-15", 5.0),  # erster Tag des Folgejahres
    )

    table, defects = create_family_hours_table(hours, names, KITA_YEAR)

    assert defects.empty
    assert table.iloc[0]["Stunden IST"] == 0.0
