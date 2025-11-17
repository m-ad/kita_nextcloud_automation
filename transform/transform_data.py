import numpy as np
import pandas as pd


def create_family_hours_table(
    df_hours: pd.DataFrame,
    df_names: pd.DataFrame,
    kita_year: int,
) -> pd.DataFrame:
    """Create a family hours table by merging hours data with names data.

    Parameters
    ----------
    hours_df:
        DataFrame containing per-person work hours data.
    names_df:
        DataFrame containing names and adresses.

    Returns
    -------
    pd.DataFrame
        Merged DataFrame with family hours and names.
    """
    # Define target hours per week based on age and single-parent status
    target_hours_dict = {
        (False, 1): 102,
        (False, 2): 132,
        (False, 3): 132,  # to be determined
        (True, 1): 50,
        (True, 2): 60,
    }

    # filter for Kita year
    df_hours = df_hours.astype({"Datum": "datetime64[s]"}).query(
        f"'{kita_year}-09-01'<Datum<'{kita_year + 1}-09-01'"
    )

    # Add family column
    df_names = df_names.assign(
        Familie=np.where(
            (df_names["Nachname Mutter"] == df_names["Nachname Vater"])
            | df_names["Nachname Vater"].isna(),
            df_names["Nachname Mutter"],
            df_names["Nachname Mutter"] + " & " + df_names["Nachname Vater"],
        )
    ).assign(alleinerziehend=lambda x: x["Nachname Vater"].isna())
    # TODO: This ignores the case of single-parent fathers for now!

    # create a dictionary Nextcloud ID -> total hours worked, e.g. {"m.meier": 37.5, "e.schmidt": 12.0}
    hours_dict: dict[str, float] = (
        df_hours.groupby("wer?_id")["Stunden"].sum().to_dict()
    )

    # create a dictionary family name -> children count, e.g. {"Musterfamilie": 2}
    children_count: dict[str, int] = (
        df_names.groupby("Familie")["Vorname Kind"].count().sort_values().to_dict()
    )

    # create family hours table
    # TODO: break this up for better readability and documentation
    family_hours = (
        df_names[
            [
                "Familie",
                "alleinerziehend",
                "Nextcloudaccount Mutter",
                "Nextcloudaccount Vater",
            ]
        ]
        .melt(id_vars=["Familie", "alleinerziehend"], value_name="nextcloud_account")
        .drop(columns="variable")
        .assign(n_children=lambda x: x["Familie"].map(children_count))
        .assign(
            target_hours=lambda x: x.apply(
                lambda row: target_hours_dict[
                    (row["alleinerziehend"], row["n_children"])
                ],
                axis=1,
            )
        )
        .assign(actual_hours=lambda x: x["nextcloud_account"].map(hours_dict).fillna(0))
        .groupby(["Familie", "alleinerziehend", "target_hours", "n_children"])
        .sum(numeric_only=True)
        .reset_index()
        .drop(columns=["alleinerziehend", "n_children"])
        .assign(progress=lambda x: x["actual_hours"] / x["target_hours"] * 100)
        .astype({"progress": int})
        .sort_values(by="progress", ascending=False)
        .reset_index(drop=True)
        .rename(
            columns={
                "target_hours": "Stunden SOLL",
                "actual_hours": "Stunden IST",
                "progress": "Fortschritt",
                # "n_children": "Anzahl Kinder",
            },
            errors="ignore",
        )
        .assign(Fortschritt=lambda x: np.minimum(x["Fortschritt"], 100))
    )
    return family_hours
