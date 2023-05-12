# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 19:05:26 2022

@author: anne
"""
import copy
import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"

os.chdir("/Users/anne/OneDrive/Data")

stationid = "428"

# max and min values for station POR
por = pd.read_csv("NDOT_Study/Nev_Snotel_summary_new.csv")
por = por.loc[por["station_num"] == int(stationid)]

SWEmax = int(por["snow_water_equiv_max"])
SWEmin = 0  # mm
depthmax = int(por["snow_depth_max"])
depth1hrmax = 10
depthmin = 0  # cm
precipseasonmax = int(por["precip_acc_max"])
precipseasonmin = 0  # mm
precip24hrmax = int(por["precip_max"])
precip24hrmin = 0  # mm/hr
precip1hrmax = 10  # mm/hr
precip1hrmin = 0  # mm/hr
tempmax = int(por["air_temp_max"])
tempmin = int(por["air_temp_min"])

for year in range(
    2006, 2008
):  # this loops i through from WYstart (included) to WYend (not included) in 1 year chunks
    # read L0 data
    snotel_hourly = pd.read_csv(
        "Level0_SNOTEL/" + stationid + "_WY" + str(year) + "_L0.csv",
        parse_dates=["date"],
    )
    # snotel_hourly.columns =['date','sh_temp_C','sh_temp_qc','sh_precip_mm','sh_precip_qc','sh_snowdepth_cm','sh_snowdepth_qc','sh_SWE_mm','sh_SWE_qc','sh_sm2_pct','sh_sm2_qc','sh_sm8_pct','sh_sm8_qc','sh_sm20_pct','sh_sm20_qc','sh_st2_C','sh_st2_qc','sh_st8_C','sh_st8_qc','sh_st20_C','sh_st20_qc']
    snotel_hourly = snotel_hourly.set_index("date")

    # read daily SNOTEL data
    snotel_daily = pd.read_csv(
        "SNOTEL_D/sd_" + stationid + "_WY" + str(year) + ".csv", parse_dates=["Date"]
    )  # data is start of day values
    # name columns with unique sd (snotel daily) identifier
    snotel_daily.columns = [
        "date",
        "sd_temp_avg_C",
        "sd_temp_avg_qc",
        "sd_temp_max_C",
        "sd_temp_max_qc",
        "sd_temp_min_C",
        "sd_temp_min_qc",
        "sd_temp_obs_C",
        "sd_temp_obs_qc",
        "sd_precip_mm",
        "sd_precip_qc",
        "sd_precip_24hr_mm",
        "sd_precip_24hr_qc",
        "sd_precip_24hrsnowadj_mm",
        "sd_precip_24hradj_qc",
        "sd_depth_cm",
        "sd_depth_qc",
        "sd_SWE_mm",
        "sd_SWE_qc",
        "sd_sm2_pct",
        "sd_sm2_qc",
        "sd_sm8_pct",
        "sd_sm8_qc",
        "sd_sm20_pct",
        "sd_sm20_qc",
        "sd_sm2_avg_pct",
        "sd_sm2_avg_qc",
        "sd_sm8_avg_pct",
        "sd_sm8_avg_qc",
        "sd_sm20_avg_pct",
        "sd_sm20_avg_qc",
        "sd_sm2_max_pct",
        "sd_sm2_max_qc",
        "sd_sm8_max_pct",
        "sd_sm8_max_qc",
        "sd_sm20_max_pct",
        "sd_sm20_max_qc",
        "sd_sm2_min_pct",
        "sd_sm2_min_qc",
        "sd_sm8_min_pct",
        "sd_sm8_min_qc",
        "sd_sm20_min_pct",
        "sd_sm20_min_qc",
        "sd_st2_avg_pct",
        "sd_st2_avg_qc",
        "sd_st8_avg_pct",
        "sd_st8_avg_qc",
        "sd_st20_avg_pct",
        "sd_st20_avg_qc",
        "sd_st2_max_pct",
        "sd_st2_max_qc",
        "sd_st8_max_pct",
        "sd_st8_max_qc",
        "sd_st20_max_pct",
        "sd_st20_max_qc",
        "sd_st2_min_pct",
        "sd_st2_min_qc",
        "sd_st8_min_pct",
        "sd_st8_min_qc",
        "sd_st20_min_pct",
        "sd_st20_min_qc",
        "sd_st2_pct",
        "sd_st2_qc",
        "sd_st8_pct",
        "sd_st8_qc",
        "sd_st20_pct",
        "sd_st20_qc",
        "sd_density_pct",
        "sd_density_qc",
        "sd_snowrainratio",
        "sd_snowrainratio_qc",
    ]
    snotel_daily["date"] = snotel_daily["date"].apply(
        lambda x: pd.Timestamp(x).replace(hour=0, minute=0, second=0)
    )  # give the daily data a time stamp of midnight
    snotel_daily = (
        snotel_daily.set_index("date")
        .resample("H")
        .interpolate(method="linear", limit_direction="backward")
        .drop(
            [
                "sd_density_pct",
                "sd_density_qc",
                "sd_snowrainratio",
                "sd_snowrainratio_qc",
            ],
            axis=1,
        )
    )
    snotel_daily = snotel_daily.fillna(
        method="ffill"
    )  # Apply daily data QC check to each time stamp for automated QC.

    # create QC dataframe and reset all indexes to plot later
    df_qc = pd.concat([snotel_hourly, snotel_daily], axis=1)
    df_qc = df_qc.reset_index()
    snotel_hourly = snotel_hourly.reset_index()
    snotel_daily = snotel_daily.reset_index()

    ##### Level 1 #####
    #### SWE ####
    # Range check
    df_qc["sh_SWE_mm_L1"] = np.where(
        (df_qc["sh_SWE_mm"] < SWEmin), 0, df_qc["sh_SWE_mm"]
    )
    df_qc["sh_SWE_mm_L1"] = np.where(
        (df_qc["sh_SWE_mm_L1"] > SWEmax), np.nan, df_qc["sh_SWE_mm_L1"]
    )
    # Rate of change check
    df_qc["sh_SWE_mm_L1"] = np.where(
        (df_qc["sh_SWE_mm_L1"].diff() > 25), np.nan, df_qc["sh_SWE_mm_L1"]
    )
    df_qc["sh_SWE_mm_L1"] = np.where(
        (df_qc["sh_SWE_mm_L1"].diff() < -25), np.nan, df_qc["sh_SWE_mm_L1"]
    )
    # Set L1 QC Flag
    df_qc["sh_SWE_qc_L1"] = np.where(
        (df_qc["sh_SWE_mm_L1"] == np.nan), "S", df_qc["sh_SWE_qc"]
    )
    # Set L1 QA Flags
    df_qc["sh_SWE_qa_L1"] = "F"

    #### Snow Depth ####
    # Range check
    df_qc["sh_snowdepth_cm_L1"] = np.where(
        (df_qc["sh_snowdepth_cm"] < depthmin), np.nan, df_qc["sh_snowdepth_cm"]
    )
    df_qc["sh_snowdepth_cm_L1"] = np.where(
        (df_qc["sh_snowdepth_cm"] > depthmax), np.nan, df_qc["sh_snowdepth_cm_L1"]
    )
    # Rate of change check
    df_qc["sh_snowdepth_cm_L1"] = np.where(
        (df_qc["sh_snowdepth_cm"].diff() > 25), np.nan, df_qc["sh_snowdepth_cm_L1"]
    )
    df_qc["sh_snowdepth_cm_L1"] = np.where(
        (df_qc["sh_snowdepth_cm"].diff() < -25), np.nan, df_qc["sh_snowdepth_cm_L1"]
    )
    # Fill missing data caused by painting during snowfall with linear interpolation
    df_qc["sh_snowdepth_cm_L1"] = df_qc["sh_snowdepth_cm_L1"].interpolate(
        method="linear", limit_direction="backward"
    )
    # Set L1 QC Flag
    df_qc["sh_snowdepth_qc_L1"] = np.where(
        (df_qc["sh_snowdepth_cm_L1"] == np.nan), "S", df_qc["sh_snowdepth_qc"]
    )
    # Set L1 QA Flags
    df_qc["sh_snowdepth_qa_L1"] = "F"

    #### Precip - EXPERIMENTAL ####
    # Range check
    df_qc["sh_precip_mm_L1"] = np.where(
        (df_qc["sh_precip_mm"] < precipseasonmin), 0, df_qc["sh_precip_mm"]
    )
    df_qc["sh_precip_mm_L1"] = np.where(
        (df_qc["sh_precip_mm_L1"] > precipseasonmax), np.nan, df_qc["sh_precip_mm_L1"]
    )
    # rate of change check --> not ready to be incorporated
    # df_qc['sh_precip_mm_L1'] = np.where((df_qc['sh_precip_mm'].diff() > 100) , np.nan, df_qc['sh_precip_mm_L1'])
    # df_qc['sh_precip_mm_L1'] = np.where((df_qc['sh_precip_mm'].diff() < -100) , np.nan, df_qc['sh_precip_mm_L1'])
    # Range check QC Flag
    df_qc["sh_precip_qc_L1"] = np.where(
        (df_qc["sh_precip_mm_L1"] < precipseasonmin), "S", df_qc["sh_precip_qc"]
    )
    df_qc["sh_precip_qc_L1"] = np.where(
        (df_qc["sh_precip_mm_L1"] > precipseasonmax), "S", df_qc["sh_precip_qc_L1"]
    )
    df_qc["sh_precip_qc_L1"] = np.where(
        (df_qc["sh_precip_mm_L1"].diff() < precip1hrmin), "S", df_qc["sh_precip_qc_L1"]
    )
    df_qc["sh_precip_qc_L1"] = np.where(
        (df_qc["sh_precip_mm_L1"].diff() > precip1hrmax), "S", df_qc["sh_precip_qc_L1"]
    )
    df_qc["sh_precip_qc_L1"] = np.where(
        (df_qc["sh_precip_mm_L1"] == np.nan), "S", df_qc["sh_precip_qc"]
    )
    # Set L1 QA Flags
    df_qc["sh_precip_qa_L1"] = "F"

    #### Temp ####
    # NOAA9
    df_qc["sh_temp_C_L1"] = (
        610558.226380138 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 9
        - 2056177.65461394 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 8
        + 2937046.42906361 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 7
        - 2319657.12916417 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 6
        + 1111854.33825836 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 5
        - 337069.883250001 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 4
        + 66105.7015922199 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 3
        - 8386.78320604513 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45) ** 2
        + 824.818021779729 * (((df_qc["sh_temp_C"] + 65.929)) / 194.45)
        - 86.7321006757439
    )
    df_qc["sh_temp_C_L1"] = np.where(
        (df_qc["sh_temp_C_L1"] < tempmin), np.nan, df_qc["sh_temp_C_L1"]
    )
    df_qc["sh_temp_C_L1"] = np.where(
        (df_qc["sh_temp_C_L1"] > tempmax), np.nan, df_qc["sh_temp_C_L1"]
    )
    # set all values to edited
    df_qc["sh_temp_qc_L1"] = "E"
    # Set L 1 QC Flag
    df_qc["sh_temp_qc_L1"] = np.where(
        (df_qc["sh_temp_C_L1"] > tempmax), "S", df_qc["sh_temp_qc_L1"]
    )
    df_qc["sh_temp_qc_L1"] = np.where(
        (df_qc["sh_temp_C_L1"] < tempmin), "S", df_qc["sh_temp_qc_L1"]
    )
    df_qc["sh_temp_qc_L1"] = np.where(
        (df_qc["sh_temp_C_L1"] == np.nan), "S", df_qc["sh_temp_qc_L1"]
    )
    # Set L1 QA Flags
    df_qc["sh_temp_qa_L1"] = "F"

    #### Soil Moisture ####
    df_qc["sh_sm2_pct_L1"] = df_qc["sh_sm2_pct"]  # already QCd by NRCS
    df_qc["sh_sm8_pct_L1"] = df_qc["sh_sm8_pct"]  # already QC'd by NRCS
    df_qc["sh_sm20_pct_L1"] = df_qc["sh_sm20_pct"]  # already QC'd by NRCS
    df_qc["sh_sm2_qc_L1"] = "V"
    df_qc["sh_sm8_qc_L1"] = "V"
    df_qc["sh_sm20_qc_L1"] = "V"
    df_qc["sh_sm2_qa_L1"] = "A"
    df_qc["sh_sm8_qa_L1"] = "A"
    df_qc["sh_sm20_qa_L1"] = "A"

    #### Soil Temperature ####
    df_qc["sh_st2_C_L1"] = df_qc["sh_st2_C"]  # already QC'd by NRCS
    df_qc["sh_st8_C_L1"] = df_qc["sh_st8_C"]  # already QC'd by NRCS
    df_qc["sh_st20_C_L1"] = df_qc["sh_st20_C"]  # already QC'd by NRCS
    df_qc["sh_st2_qc_L1"] = "V"
    df_qc["sh_st8_qc_L1"] = "V"
    df_qc["sh_st20_qc_L1"] = "V"
    df_qc["sh_st2_qa_L1"] = "A"
    df_qc["sh_st8_qa_L1"] = "A"
    df_qc["sh_st20_qa_L1"] = "A"

    # Create Level 1 Data
    level1 = copy.deepcopy(df_qc)
    level1 = level1.round(1)  # round to one decimal
    # select the columns and order of parameters included
    level1 = level1[
        [
            "date",
            "sh_SWE_mm_L1",
            "sh_SWE_qc_L1",
            "sh_SWE_qa_L1",
            "sh_snowdepth_cm_L1",
            "sh_snowdepth_qc_L1",
            "sh_snowdepth_qa_L1",
            "sh_precip_mm_L1",
            "sh_precip_qc_L1",
            "sh_precip_qa_L1",
            "sh_temp_C_L1",
            "sh_temp_qc_L1",
            "sh_temp_qa_L1",
            "sh_sm2_pct_L1",
            "sh_sm2_qc_L1",
            "sh_sm2_qa_L1",
            "sh_sm8_pct_L1",
            "sh_sm8_qc_L1",
            "sh_sm8_qa_L1",
            "sh_sm20_pct_L1",
            "sh_sm20_qc_L1",
            "sh_sm20_qa_L1",
            "sh_st2_C_L1",
            "sh_st2_qc_L1",
            "sh_st2_qa_L1",
            "sh_st8_C_L1",
            "sh_st8_qc_L1",
            "sh_st8_qa_L1",
            "sh_st20_C_L1",
            "sh_st20_qc_L1",
            "sh_st20_qa_L1",
        ]
    ]
    # rename the columns so the do not have L1 at the end
    level1.columns = [
        "date",
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
    level1 = level1.set_index("date")

    # Export
    level1.to_csv(
        "/Users/anne/OneDrive/Data/Level1_SNOTEL/"
        + stationid
        + "_WY"
        + str(year)
        + "_L1.csv"
    )

    ##### Level 2 #####
    #### SWE ####
    df_qc["sh_SWE_mm_L2"] = df_qc["sh_SWE_mm_L1"].interpolate(
        method="linear", limit_direction="backward", limit=24
    )  # linear interpolation of data gaps but never more than 24 hours
    # Calculate guides values for dynamic smoothing and manual QC process
    df_qc["sh_SWE_auto6h_mm"] = (
        df_qc["sh_SWE_mm_L2"].rolling(6, center=True, closed="right").median()
    )
    df_qc["sh_SWE_6hdiff"] = df_qc[
        "sh_SWE_auto6h_mm"
    ].diff()  # to fill in manual difference in excell once SWE is verified
    df_qc["sh_SWE_auto24h_mm"] = df_qc.sh_SWE_mm_L2.rolling(
        24, center=True, closed="right"
    ).median()
    df_qc["sh_SWE_24hdiff"] = df_qc["sh_SWE_auto24h_mm"].diff()
    # Dynamic smoothing and daily data check
    df_qc["sh_SWE_mm_L2"] = df_qc["sh_SWE_auto6h_mm"]
    df_qc["sh_SWE_mm_L2"] = np.where(
        (df_qc["sh_SWE_24hdiff"] <= 0),
        df_qc["sh_SWE_auto24h_mm"],
        df_qc["sh_SWE_mm_L2"],
    )  # if hourly SWE has no change or is decreasing over 24 hours, apply 24 hr rolling median to smooth diurnal flutter
    df_qc["sh_SWE_mm_L2"] = np.where(
        (df_qc["sd_SWE_mm"].diff() <= 0),
        df_qc["sh_SWE_auto24h_mm"],
        df_qc["sh_SWE_mm_L2"],
    )  # if daily SWE has no change or is decreasing, then apply 24 hr rolling median to smooth diurnal flutter - yes, you need both versions!
    df_qc["sh_SWE_mm_L2"] = np.where(
        (df_qc["sd_temp_max_C"] > 7), df_qc["sh_SWE_auto24h_mm"], df_qc["sh_SWE_mm_L2"]
    )  # Smooth diurnal flutter for any day with maximum air temp over 7 °C
    df_qc["sh_SWE_mm_L2"] = np.where(
        (df_qc["sd_SWE_mm"] == 0), 0, df_qc["sh_SWE_mm_L2"]
    )  # if daily QC product is 0 then set all hourly data to 0
    # QA/QC Flag
    df_qc["sh_SWE_qc_L2"] = np.where(
        (df_qc["sh_SWE_mm_L2"] == np.nan), "S", df_qc["sh_SWE_qc_L1"]
    )
    df_qc["sh_SWE_qc_L2"] = np.where(
        (df_qc["sh_SWE_mm"] == df_qc["sh_SWE_mm_L2"]) & (df_qc["sh_SWE_mm_L2"] != "S"),
        "V",
        "E",
    )  # if Level 2 data equals the origional data and the flag has not been set to suspect, then flag as "V", otherwise the data has been edited "E".
    df_qc["sh_SWE_qa_L2"] = "F"  # Flag data QA as passing automated QC

    #### Snow Depth ####
    df_qc["sh_snowdepth_cm_L2"] = df_qc["sh_snowdepth_cm_L1"].interpolate(
        method="linear", limit_direction="backward", limit=24
    )  # linear interpolation of data gaps but never more than 24 hours
    # Calculate median values for manual QC
    df_qc["sh_snowdepth_auto6h_cm"] = df_qc.sh_snowdepth_cm_L2.rolling(
        6, center=True, closed="right"
    ).median()
    df_qc["sh_snowdepth_6hdiff"] = df_qc["sh_snowdepth_auto6h_cm"].diff()
    df_qc["sh_snowdepth_auto12h_cm"] = df_qc.sh_snowdepth_cm_L2.rolling(
        12, center=True, closed="right"
    ).median()
    df_qc["sh_snowdepth_12hdiff"] = df_qc["sh_snowdepth_auto12h_cm"].diff()
    df_qc["sh_snowdepth_auto24h_cm"] = df_qc.sh_snowdepth_cm_L2.rolling(
        24, center=True, closed="right"
    ).median()
    df_qc["sh_snowdepth_24hdiff"] = df_qc["sh_snowdepth_auto24h_cm"].diff()
    # Dynamic smoothing
    df_qc["sh_snowdepth_cm_L2"] = df_qc["sh_snowdepth_auto6h_cm"]
    df_qc["sh_snowdepth_cm_L2"] = np.where(
        (df_qc["sh_snowdepth_24hdiff"] <= 0),
        df_qc["sh_snowdepth_auto24h_cm"],
        df_qc["sh_snowdepth_cm_L2"],
    )  # no change in depth or compaction/melting then apply 24 hr rolling median to smooth diurnal flutter
    df_qc["sh_snowdepth_cm_L2"] = np.where(
        (df_qc["sd_temp_max_C"] > 7),
        df_qc["sh_snowdepth_auto24h_cm"],
        df_qc["sh_snowdepth_cm_L2"],
    )  # days with max temp above 7C has no precip according to CSSL findings so smooth diurnal flutter
    df_qc["sh_snowdepth_cm_L2"] = np.where(
        (df_qc["sd_depth_cm"] == 0), 0, df_qc["sh_snowdepth_cm_L2"]
    )  # if daily QC product has 0 then set all hourly data to 0
    # QA/QC Flag
    df_qc["sh_snowdepth_qc_L2"] = np.where(
        (df_qc["sh_snowdepth_cm_L2"] == np.nan), "S", df_qc["sh_snowdepth_qc_L1"]
    )
    df_qc["sh_snowdepth_qc_L2"] = np.where(
        (df_qc["sh_snowdepth_cm"] == df_qc["sh_snowdepth_cm_L2"])
        & (df_qc["sh_snowdepth_qc_L2"] != "S"),
        "V",
        "E",
    )  # if Level 2 data equals the origional data and the flag has not been set to suspect, then flag as "V", otherwise the data has been edited "E".
    df_qc["sh_snowdepth_qa_L2"] = "F"  # Flag data QA as passing automated QC

    #### Precip - EXPERIMENTAL ####
    df_qc["sh_precip_mm_L2"] = df_qc["sh_precip_mm_L1"].interpolate(
        method="linear", limit_direction="backward", limit=24
    )  # backfill remaining data gaps
    # Calculate median values for manual QC
    df_qc["sh_precip_auto24h_mm"] = df_qc.sh_precip_mm_L2.rolling(
        24, center=True, closed="right"
    ).median()
    df_qc["sh_precip_24hrdiff"] = df_qc["sh_precip_auto24h_mm"].diff()
    # Dynamic smoothing
    df_qc["sh_precip_mm_L2"] = df_qc.sh_precip_mm_L2.rolling(
        6, center=True, closed="right"
    ).median()
    df_qc["sh_precip_mm_L2"] = np.where(
        (df_qc["sh_precip_24hrdiff"] <= 0),
        df_qc["sh_precip_auto24h_mm"],
        df_qc["sh_precip_mm_L2"],
    )  # no change in precip or sligth decrease from temp swing then apply 24 hr rolling median to smooth diurnal flutter
    df_qc["sh_precip_mm_L2"] = np.where(
        (df_qc["sd_temp_max_C"] > 7),
        df_qc["sh_precip_auto24h_mm"],
        df_qc["sh_precip_mm_L2"],
    )  # days with max temp above 7C has no precip according to CSSL findings so smooth diurnal flutter
    df_qc["sh_precip_mm_L2"] = np.where(
        (df_qc["sd_precip_mm"] == 0), 0, df_qc["sh_precip_mm_L2"]
    )  # if daily QC product has 0 then set all hourly data to 0
    df_qc["sh_precip_mm_L2"] = df_qc[
        "sh_precip_mm_L2"
    ].diff()  # treat data as 1 hour incremental data to select when to use precip data or use SWE data
    df_qc["sh_precip_mm_L2"] = np.where(
        (df_qc["sd_precip_24hr_mm"] == 0), 0, df_qc["sh_precip_mm_L2"]
    )
    df_qc["sh_precip_mm_L2"] = np.where(
        (df_qc["sh_precip_mm_L2"] < -10), 0, df_qc["sh_precip_mm_L2"]
    )  # remove major decreases in precip data
    df_qc["sh_precip_mm_L2"] = np.where(
        (df_qc["sh_precip_mm_L2"] > 50), 0, df_qc["sh_precip_mm_L2"]
    )  # remove maintenance data but will also remove major snow plug release data
    # df_qc['sh_precip_mm_L2'] = np.where((df_qc['sh_precip_mm_L2'] > 10), 0, df_qc['sh_precip_mm_L2']) #comment out to keep snow plug releases
    df_qc["sh_precip_mm_L2"] = df_qc["sh_precip_mm_L2"].cumsum()  # add it all up
    # QA/QC Flag
    df_qc["sh_precip_qc_L2"] = np.where(
        (df_qc["sh_precip_mm_L2"] == np.nan), "S", df_qc["sh_precip_qc_L1"]
    )
    df_qc["sh_precip_qc_L2"] = np.where(
        (df_qc["sh_precip_mm"] == df_qc["sh_precip_mm_L2"])
        & (df_qc["sh_precip_qc_L2"] != "S"),
        "V",
        "E",
    )  # if Level 2 data equals the origional data and the flag has not been set to suspect, then flag as "V", otherwise the data has been edited "E".
    df_qc["sh_precip_qc_L2"] = np.where(
        (df_qc["sh_precip_mm_L2"].diff() < precip1hrmin), "S", df_qc["sh_precip_qc_L2"]
    )
    df_qc["sh_precip_qc_L2"] = np.where(
        (df_qc["sh_precip_mm_L2"].diff() > precip1hrmax), "S", df_qc["sh_precip_qc_L2"]
    )
    df_qc["sh_precip_qa_L2"] = "F"  # Flag data QA as passing automated QC

    #### Temperature ####
    df_qc["sh_temp_C_L2"] = df_qc["sh_temp_C_L1"]
    # QA/QC Flag
    df_qc["sh_temp_qc_L2"] = df_qc["sh_temp_qc_L1"]
    df_qc["sh_temp_qa_L2"] = "F"

    #### Soil Moisture ####
    df_qc["sh_sm2_pct_L2"] = df_qc["sh_sm2_pct_L1"]  # already QC'd by NRCS
    df_qc["sh_sm8_pct_L2"] = df_qc["sh_sm8_pct_L1"]  # already QC'd by NRCS
    df_qc["sh_sm20_pct_L2"] = df_qc["sh_sm20_pct_L1"]  # already QC'd by NRCS
    df_qc["sh_sm2_qc_L2"] = "V"
    df_qc["sh_sm8_qc_L2"] = "V"
    df_qc["sh_sm20_qc_L2"] = "V"
    df_qc["sh_sm2_qa_L2"] = "A"
    df_qc["sh_sm8_qa_L2"] = "A"
    df_qc["sh_sm20_qa_L2"] = "A"

    #### Soil Temperature ####
    df_qc["sh_st2_C_L2"] = df_qc["sh_st2_C_L1"]  # already QC'd by NRCS
    df_qc["sh_st8_C_L2"] = df_qc["sh_st8_C_L1"]  # already QC'd by NRCS
    df_qc["sh_st20_C_L2"] = df_qc["sh_st20_C_L1"]  # already QC'd by NRCS
    df_qc["sh_st2_qc_L2"] = "V"
    df_qc["sh_st8_qc_L2"] = "V"
    df_qc["sh_st20_qc_L2"] = "V"
    df_qc["sh_st2_qa_L2"] = "A"
    df_qc["sh_st8_qa_L2"] = "A"
    df_qc["sh_st20_qa_L2"] = "A"

    # Create Level 2 Data
    level2 = copy.deepcopy(df_qc)
    level2 = level2.round(1)  # round to one decimal
    level2 = level2[
        [
            "date",
            "sh_SWE_mm_L2",
            "sh_SWE_qc_L2",
            "sh_SWE_qa_L2",
            "sh_snowdepth_cm_L2",
            "sh_snowdepth_qc_L2",
            "sh_snowdepth_qa_L2",
            "sh_precip_mm_L2",
            "sh_precip_qc_L2",
            "sh_precip_qa_L2",
            "sh_temp_C_L2",
            "sh_temp_qc_L2",
            "sh_temp_qa_L2",
            "sh_sm2_pct_L2",
            "sh_sm2_qc_L2",
            "sh_sm2_qa_L2",
            "sh_sm8_pct_L2",
            "sh_sm8_qc_L2",
            "sh_sm8_qa_L2",
            "sh_sm20_pct_L2",
            "sh_sm20_qc_L2",
            "sh_sm20_qa_L2",
            "sh_st2_C_L2",
            "sh_st2_qc_L2",
            "sh_st2_qa_L2",
            "sh_st8_C_L2",
            "sh_st8_qc_L2",
            "sh_st8_qa_L2",
            "sh_st20_C_L2",
            "sh_st20_qc_L2",
            "sh_st20_qa_L2",
        ]
    ]
    level2.columns = [
        "date",
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
    level2 = level2.set_index("date")

    # Export
    level2.to_csv(
        "/Users/anne/OneDrive/Data/Level2_SNOTEL/"
        + stationid
        + "_WY"
        + str(year)
        + "_L2.csv"
    )

    # PLOT DATA
    df_qc["density"] = df_qc["sh_SWE_mm_L2"] / df_qc["sh_snowdepth_cm_L2"] * 10

    fig = go.Figure()

    # #SWE
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sd_SWE_mm"],
            line=dict(color="cornflowerblue"),
            opacity=0.5,
            name="SD SWE",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_SWE_mm"],
            line=dict(color="cornflowerblue"),
            opacity=0.25,
            name="SH SWE",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_SWE_mm_L1"],
            line=dict(color="cornflowerblue"),
            opacity=0.15,
            name="L1 SWE",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_SWE_mm_L2"],
            line=dict(color="deeppink"),
            opacity=0.5,
            name="L2 SWE",
            yaxis="y1",
        )
    )

    # #Depth
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sd_depth_cm"],
            line=dict(color="darkolivegreen"),
            opacity=0.5,
            name="SD depth",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_snowdepth_cm"],
            line=dict(color="darkolivegreen"),
            opacity=0.25,
            name="SH depth",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_snowdepth_cm_L1"],
            line=dict(color="darkolivegreen"),
            opacity=0.15,
            name="L1 depth",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_snowdepth_cm_L2"],
            line=dict(color="deeppink"),
            opacity=0.5,
            name="L2 depth",
            yaxis="y1",
        )
    )

    # #Density
    fig.add_trace(
        go.Scatter(
            mode="markers",
            x=df_qc["date"],
            y=df_qc["density"],
            marker=dict(color="lightgrey", symbol="cross", size=5),
            name="L2 density",
            yaxis="y2",
        )
    )

    # #Precip
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sd_precip_mm"],
            line=dict(color="teal"),
            opacity=0.5,
            name="SD precip",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_precip_mm"],
            line=dict(color="teal"),
            opacity=0.25,
            name="SH precip",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_precip_mm_L1"],
            line=dict(color="teal"),
            opacity=0.15,
            name="L1 precip",
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_precip_mm_L2"],
            line=dict(color="deeppink"),
            opacity=0.5,
            name="L2 precip",
            yaxis="y1",
        )
    )

    # #Temp
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_temp_C_L2"],
            line=dict(color="lightcoral"),
            name="L3 temp",
            yaxis="y2",
        )
    )

    # # Soil Moistrue
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_sm2_pct_L2"],
            line=dict(color="sandybrown"),
            name="L3 sm2",
            yaxis="y2",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_sm8_pct_L2"],
            line=dict(color="chocolate"),
            name="L3 sm8",
            yaxis="y2",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_sm20_pct_L2"],
            line=dict(color="saddlebrown"),
            name="L3 sm20",
            yaxis="y2",
        )
    )

    # Soil Temperature
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_st2_C_L2"],
            line=dict(color="lightgrey"),
            name="L3 st2",
            yaxis="y2",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_st8_C_L2"],
            line=dict(color="darkgrey"),
            name="L3 st8",
            yaxis="y2",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_qc["date"],
            y=df_qc["sh_st20_C_L2"],
            line=dict(color="dimgrey"),
            name="L3 st20",
            yaxis="y2",
        )
    )

    fig.update_layout(
        title=stationid,
        # xaxis_title='X Axis Title',
        yaxis=dict(title="SWE (mm) / Precip (mm) / Depth (cm)"),
        yaxis2=dict(title="Temp (C) / Soil Moistuce (%)", overlaying="y", side="right"),
        legend_title="Legend",
        plot_bgcolor="#FBFBFB",
    )
    fig.show()

    level2QC = copy.deepcopy(df_qc)
    level2QC = level2QC.round(1)  # round to one decimal
    level2QC["sh_SWE_L2diff"] = level2QC["sh_SWE_mm_L2"].diff()
    level2QC["sh_snowdepth_L2diff"] = level2QC["sh_snowdepth_cm_L2"].diff()
    level2QC["sh_precip_diff"] = level2QC["sh_precip_mm"].diff()
    level2QC["sh_precip_L2diff"] = level2QC["sh_precip_mm_L2"].diff()
    # add level 3 data to manually edit in excel
    level2QC["sh_SWE_mm_L3"] = level2QC["sh_SWE_mm_L2"]
    level2QC["sh_precip_mm_L3"] = level2QC["sh_precip_mm_L2"]
    level2QC["sh_snowdepth_cm_L3"] = level2QC["sh_snowdepth_cm_L2"]

    level2QC = level2QC[
        [
            "date",
            "sh_SWE_mm",
            "sh_SWE_qc",
            "sh_SWE_auto6h_mm",
            "sh_SWE_6hdiff",
            "sh_SWE_auto24h_mm",
            "sh_SWE_24hdiff",
            "sh_SWE_mm_L2",
            "sh_SWE_L2diff",
            "sh_SWE_mm_L2",
            "sh_SWE_qc_L2",
            "sh_SWE_qa_L2",
            "sh_snowdepth_cm",
            "sh_snowdepth_qc",
            "sh_snowdepth_auto6h_cm",
            "sh_snowdepth_auto12h_cm",
            "sh_snowdepth_auto24h_cm",
            "sh_snowdepth_cm_L2",
            "sh_snowdepth_cm_L3",
            "sh_snowdepth_qc_L2",
            "sh_snowdepth_qa_L2",
            "sh_precip_mm",
            "sh_precip_diff",
            "sh_precip_qc",
            "sh_precip_auto24h_mm",
            "sh_precip_24hrdiff",
            "sh_precip_mm_L2",
            "sh_precip_L2diff",
            "sh_precip_mm_L3",
            "sh_precip_qc_L2",
            "sh_precip_qa_L2",
            "sh_temp_C_L2",
            "sh_temp_qc_L2",
            "sh_temp_qa_L2",
            "sh_sm2_pct_L2",
            "sh_sm2_qc_L2",
            "sh_sm2_qa_L2",
            "sh_sm8_pct_L2",
            "sh_sm8_qc_L2",
            "sh_sm8_qa_L2",
            "sh_sm20_pct_L2",
            "sh_sm20_qc_L2",
            "sh_sm20_qa_L2",
            "sh_st2_C_L2",
            "sh_st2_qc_L2",
            "sh_st2_qa_L2",
            "sh_st8_C_L2",
            "sh_st8_qc_L2",
            "sh_st8_qa_L2",
            "sh_st20_C_L2",
            "sh_st20_qc_L2",
            "sh_st20_qa_L2",
        ]
    ]

    level2QC.columns = [
        "date",
        "sh_SWE_mm",
        "sh_SWE_qc",
        "sh_SWE_auto6h_mm",
        "sh_SWE_6hdiff",
        "sh_SWE_auto24h_mm",
        "sh_SWE_24hdiff",
        "sh_SWE_mm_L2",
        "sh_SWE_L2diff",
        "sh_SWE_mm_L3",
        "sh_SWE_qc_L3",
        "sh_SWE_qa_L3",
        "sh_snowdepth_cm",
        "sh_snowdepth_qc",
        "sh_snowdepth_auto6h_cm",
        "sh_snowdepth_auto12h_cm",
        "sh_snowdepth_auto24h_cm",
        "sh_snowdepth_cm_L2",
        "sh_snowdepth_cm_L3",
        "sh_snowdepth_qc_L3",
        "sh_snowdepth_qa_L3",
        "sh_precip_mm",
        "sh_precip_diff",
        "sh_precip_qc",
        "sh_precip_auto24h_mm",
        "sh_precip_24hrdiff",
        "sh_precip_mm_L2",
        "sh_precip_L2diff",
        "sh_precip_mm_L3",
        "sh_precip_qc_L3",
        "sh_precip_qa_L3",
        "sh_temp_C_L3",
        "sh_temp_qc_L3",
        "sh_temp_qa_L3",
        "sh_sm2_pct_L3",
        "sh_sm2_qc_L3",
        "sh_sm2_qa_L3",
        "sh_sm8_pct_L3",
        "sh_sm8_qc_L3",
        "sh_sm8_qa_L3",
        "sh_sm20_pct_L3",
        "sh_sm20_qc_L3",
        "sh_sm20_qa_L3",
        "sh_st2_C_L3",
        "sh_st2_qc_L3",
        "sh_st2_qa_L3",
        "sh_st8_C_L3",
        "sh_st8_qc_L3",
        "sh_st8_qa_L3",
        "sh_st20_C_L3",
        "sh_st20_qc_L3",
        "sh_st20_qa_L3",
    ]

    level2QC = level2QC.set_index("date")
    # export
    level2QC.to_csv(
        "/Users/anne/OneDrive/Data/Level2QC_SNOTEL/"
        + stationid
        + "_WY"
        + str(year)
        + "_L2QC.csv"
    )
