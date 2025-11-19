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
        .assign(
            target_hours=lambda x: x.apply(
                lambda row: target_hours_dict[
                    (row["alleinerziehend"], row["n_children"])
                ],
                axis=1,
            )
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
    return family_hours
