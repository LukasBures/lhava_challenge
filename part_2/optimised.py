from typing import Final

import numpy as np
import pandas as pd
from pandas import DataFrame

FEE: Final[float] = 0.000005

# Updated:
# Renamed column names according to the data
# Documented the code
# Removed unnecessary imports
# Removed unused code
# Applied black, isort, and flake8
# Extracted constant FEE and make it Final (could be a parameter of the function with default value)


def calc_pnl(all_tx: DataFrame, wallet_address: str) -> DataFrame:
    """
    Calculate the pnl of our wallet

    :param all_tx: Dataframe containing all transactions
    :param wallet_address: Address of the wallet to calculate pnl for
    :return: Dataframe with pnl, return, and holding_period [s] columns added
    """

    # Isolate the buy/sales
    # Updated: Used the loc method to select only the rows that meet the condition of the Order Type column. This is
    # generally faster than using boolean indexing with square brackets. The copy() method is added at the end to ensure
    # that a new copy of the DataFrame is created. This is because modifying a view of a DataFrame can have unexpected
    # consequences.
    data = all_tx.loc[all_tx["Order Type"] == "buyNow"].copy()

    # Define new columns for net pnl, return and holding period
    # Updated: created a list of column names (in this case, 'pnl', 'return', and 'holding_period [s]') and assigns NaN
    # values to all of them in the DataFrame data
    data[["pnl", "return", "holding_period [s]"]] = np.nan

    # Updated: Vectorised for and while loops

    # Sort by NFT and Blocktime
    data.sort_values(by=["NFT", "Blocktime"], inplace=True)

    # Extract relevant rows
    buy_rows = data.iloc[::2][data.iloc[::2]["Buyer Address"] == wallet_address]
    sell_rows = data.iloc[1::2][data.iloc[1::2]["Seller Address"] == wallet_address]

    # Calculate the net pnl for each transaction
    buy_price = buy_rows["Price"].values
    sell_price = sell_rows["Price"].values
    net_profit = sell_price - buy_price
    holding_period = sell_rows["Blocktime"].values - buy_rows["Blocktime"].values
    pcnt_profit = net_profit / buy_price

    # Save the net pnl to the original dataframe
    data.loc[sell_rows.index, "pnl"] = net_profit
    data.loc[sell_rows.index, "return"] = pcnt_profit
    data.loc[sell_rows.index, "holding_period [s]"] = holding_period

    # Convert blocktime to datetime
    data["datetime"] = pd.to_datetime(data["Blocktime"], unit="s")

    # Sort by blocktime
    data.sort_values(by="Blocktime", inplace=True)

    # Calculate the network fees in-between snapshots
    fees = []
    for i in range(len(data)):
        mask = np.logical_and(
            all_tx["Blocktime"] > data["Blocktime"].shift().iloc[i],
            all_tx["Blocktime"] <= data["Blocktime"].iloc[i],
        )
        fees.append(FEE * np.sum(mask))
    data["network_fee"] = fees

    return data
