import datetime

import numpy as np
import pandas as pd

# Updated: the column names were changed to the provided data_2.xlsx


def calc_pnl(
    dfAllTnx: pd.DataFrame, wallet_address
) -> pd.DataFrame:  # api: MagicEdenAPI):

    # NOTE: ( was missing here "pd.to_datetime(dfAllTnx"
    dfAllTnx["datetime"] = pd.to_datetime(dfAllTnx["Blocktime"] * 1000, unit="ms")
    """Calculate the pnl of our wallet"""

    # Isolate the buy/sales
    dfAct = dfAllTnx[dfAllTnx["Order Type"] == "buyNow"]
    # Get a list of each unique nft traded
    oids = dfAct["NFT"].unique().tolist()
    # Define a new column of net pnl
    dfAct["pnl"] = np.NaN
    dfAct["return"] = np.NaN
    # For each unique oid traded process the trade and record net round-trip pnl as a new column entry
    dfAct.sort_values(by="Blocktime", inplace=True, ignore_index=True)
    for oid in oids:
        df = dfAct[dfAct["NFT"] == oid]
        df.reset_index(drop=False, names="idx", inplace=True)

        # Skip tnx if not fully executed
        if len(df) < 2:
            continue

        # Iterate over the rows of tnx for the oid and
        #   calculate the pnl on each row where prior
        #   tnx was a buy and current tnx is a sale

        i = 1
        while i < len(df):
            last_tnx_is_buy = df.at[i - 1, "Buyer Address"] == wallet_address
            i_tnx_is_sell = df.at[i, "Seller Address"] == wallet_address
            if last_tnx_is_buy and i_tnx_is_sell:
                df.at[i - 1, "side"] = "buy"
                df.at[i, "side"] = "sell"
                # calc the net pnl
                NET_PROFIT = df.at[i, "Price"] - df.at[i - 1, "Price"]
                HOLDING_PERIOD = df.at[i, "Blocktime"] - df.at[i - 1, "Blocktime"]

                PCNT_PROFIT = NET_PROFIT / df.at[i - 1, "Price"]
                # save net pnl to original dataframe
                idx = df.at[i, "idx"]
                dfAct.at[idx, "pnl"] = NET_PROFIT
                # dfAct.at[idx, 'network_fee'] = NETWORK_FEE
                dfAct.at[idx, "return"] = PCNT_PROFIT
                dfAct.at[idx, "holding_period [s]"] = HOLDING_PERIOD
            i += 2  # skip the middle tnx
    # save original dataframe
    dfAct["datetime"] = pd.to_datetime(dfAct["Blocktime"] * 1000, unit="ms")
    # Calculate the network fees inbetween snapshots after sorting by blocktime
    dfAct.sort_values(by="Blocktime", inplace=True)
    dfAct["network_fee"] = dfAct.index.to_series().apply(
        lambda i: 0.000005
        * np.sum(
            np.where(
                (dfAllTnx["Blocktime"] > dfAct.at[max(0, i - 1), "Blocktime"])
                & (dfAllTnx["Blocktime"] <= dfAct.at[i, "Blocktime"]),
                1,
                0,
            )
        )
    )
    stamp = int(datetime.datetime.utcnow().timestamp() * 1000)
    return dfAct
