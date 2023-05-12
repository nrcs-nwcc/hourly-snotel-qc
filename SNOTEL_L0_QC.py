# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 11:09:36 2023

@author: anne heggli
"""
import os

import numpy as np
import pandas as pd

os.chdir("/Users/anne/OneDrive/Data")

stations = ["428"]  # enter the station or list of stations

for year in range(
    2006, 2024
):  # this loops through WYstart (included) to WYend (not included) in 1 year chunks
    for s in stations:  # this loops through the list of stations
        # read hourly SNOTEL data
        df = pd.read_csv(
            "SNOTEL_H/sh_" + s + "_WY" + str(year) + ".csv", parse_dates=["Date"]
        )  # data is start of day values
        # name columns with unique sh (snotel hourly) identifier
        df.columns = [
            "date",
            "sh_temp_C",
            "sh_temp_qc",
            "sh_precip_mm",
            "sh_precip_qc",
            "sh_snowdepth_cm",
            "sh_snowdepth_qc",
            "sh_SWE_mm",
            "sh_SWE_qc",
            "sh_sm2_pct",
            "sh_sm2_qc",
            "sh_sm8_pct",
            "sh_sm8_qc",
            "sh_sm20_pct",
            "sh_sm20_qc",
            "sh_st2_C",
            "sh_st2_qc",
            "sh_st8_C",
            "sh_st8_qc",
            "sh_st20_C",
            "sh_st20_qc",
        ]
        df["hour"] = df[
            "date"
        ].dt.hour  # create an hour column used to remove midnight stamp.
        df = df.set_index("date")

        # remove the midnight stamp and set the initial QA and QC flags
        df["sh_precip_mm"] = np.where(
            (df["hour"] == 0), np.nan, df["sh_precip_mm"]
        )  # replace midnight stamp value with nan
        df["sh_precip_qc"] = np.where(
            (df["hour"] == 0), np.nan, df["sh_precip_qc"]
        )  # replace midnight QC flag with nan
        df["sh_precip_mm"] = np.where(
            (df["hour"] == 0),
            df["sh_precip_mm"].interpolate(
                method="linear", limit_direction="backward", limit=2
            ),
            df["sh_precip_mm"],
        )  # at fill midnight stamp with linear interpolated value only if there is data before and after the midnight stamp
        df["sh_precip_qc"] = df["sh_precip_qc"].fillna(
            "E"
        )  # fill the missing QC flag to be show it is edited
        df["sh_precip_qc"] = np.where(
            df["sh_precip_mm"].isna(), "S", df["sh_precip_qc"]
        )  # But if the the precip value is missing then flag data as suspect.
        df[
            "sh_precip_qa"
        ] = "R"  # set QA flag to "R" to raw and that there has been no real QC review

        # repeat the process for snow depth
        df["sh_snowdepth_cm"] = np.where(
            (df["hour"] == 0), np.nan, df["sh_snowdepth_cm"]
        )
        df["sh_snowdepth_qc"] = np.where(
            (df["hour"] == 0), np.nan, df["sh_snowdepth_qc"]
        )
        df["sh_snowdepth_cm"] = np.where(
            (df["hour"] == 0),
            df["sh_snowdepth_cm"].interpolate(
                method="linear", limit_direction="backward", limit=2
            ),
            df["sh_snowdepth_cm"],
        )
        df["sh_snowdepth_qc"] = df["sh_snowdepth_qc"].fillna("E")
        df["sh_snowdepth_qc"] = np.where(
            df["sh_snowdepth_cm"].isna(), "S", df["sh_snowdepth_qc"]
        )
        df["sh_snowdepth_qa"] = "R"

        # repeat the process for SWE
        df["sh_SWE_mm"] = np.where((df["hour"] == 0), np.nan, df["sh_SWE_mm"])
        df["sh_SWE_qc"] = np.where((df["hour"] == 0), np.nan, df["sh_SWE_qc"])
        df["sh_SWE_mm"] = np.where(
            (df["hour"] == 0),
            df["sh_SWE_mm"].interpolate(
                method="linear", limit_direction="backward", limit=2
            ),
            df["sh_SWE_mm"],
        )
        df["sh_SWE_qc"] = df["sh_SWE_qc"].fillna("E")
        df["sh_SWE_qc"] = np.where(df["sh_SWE_mm"].isna(), "S", df["sh_SWE_qc"])
        df["sh_SWE_qa"] = "R"

        # set the QA flag for all of the other parameters
        df["sh_temp_qa"] = "R"
        df["sh_sm2_qa"] = "R"
        df["sh_sm8_qa"] = "R"
        df["sh_sm20_qa"] = "R"
        df["sh_st2_qa"] = "R"
        df["sh_st8_qa"] = "R"
        df["sh_st20_qa"] = "R"

        df = df.drop(["hour"], axis=1)  # drop the hour column

        # select the columns to be included and in the specific order desired for export to .csv
        df = df[
            [
                "sh_SWE_mm",
                "sh_SWE_qc",
                "sh_SWE_qa",
                "sh_snowdepth_cm",
                "sh_snowdepth_qc",
                "sh_snowdepth_qa",
                "sh_precip_mm",
                "sh_precip_qc",
                "sh_precip_qa",
                "sh_temp_C",
                "sh_temp_qc",
                "sh_temp_qa",
                "sh_sm2_pct",
                "sh_sm2_qc",
                "sh_sm2_qa",
                "sh_sm8_pct",
                "sh_sm8_qc",
                "sh_sm8_qa",
                "sh_sm20_pct",
                "sh_sm20_qc",
                "sh_sm20_qa",
                "sh_st2_C",
                "sh_st2_qc",
                "sh_st2_qa",
                "sh_st8_C",
                "sh_st8_qc",
                "sh_st8_qa",
                "sh_st20_C",
                "sh_st20_qc",
                "sh_st20_qa",
            ]
        ]
        # export the file to a .csv location
        df.to_csv(
            "/Users/anne/OneDrive/Data/Level0_SNOTEL/"
            + s
            + "_WY"
            + str(year)
            + "_L0.csv"
        )
