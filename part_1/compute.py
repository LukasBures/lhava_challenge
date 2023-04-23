import numpy as np
import pandas as pd
from pandas import DataFrame, Series

# Note: stats
# data["type"].value_counts()
# type
# bid                3849102
# cancelBid           259143
# list                 37358
# buyNow                7352
# delist                7035
# auctionCreated           5
# auctionCanceled          5
# Name: count, dtype: int64


def get_volatility(data: DataFrame) -> DataFrame:
    # Calculate rolling std (volatility) over 1-hour, 6-hour, and 24-hour windows
    volatility_1h: DataFrame = data["price"].rolling(window="1h").std()
    volatility_6h: DataFrame = data["price"].rolling(window="6h").std()
    volatility_24h: DataFrame = data["price"].rolling(window="24h").std()

    # Combine volatility into new DataFrame
    volatility_data: DataFrame = pd.concat(
        [volatility_1h, volatility_6h, volatility_24h],
        axis=1,
        keys=["volatility_1h", "volatility_6h", "volatility_24h"],
    )
    return volatility_data


def get_best_bid(data: DataFrame) -> DataFrame:
    # Create a new dataframe with the same index as the input dataframe
    best_bid_data: DataFrame = DataFrame(index=data.index, columns=["best_bid"])

    # Get the bid rows
    bid_rows = data[data["type"] == "bid"]

    # Group the bid rows by index and get the maximum bid for each group
    max_bids = bid_rows.groupby(bid_rows.index)["price"].max()

    # Fill the best_bid_data dataframe with the maximum bids for each index
    best_bid_data["best_bid"] = max_bids.reindex(best_bid_data.index).fillna(method="ffill")

    # Return the best_bid_data dataframe
    return best_bid_data


def get_lowest_listing(data: DataFrame) -> DataFrame:
    # Create a new dataframe with the same index as the input dataframe
    lowest_listing_data: DataFrame = DataFrame(index=data.index, columns=["lowest_listing"])

    # Get the list rows
    list_rows = data[data["type"] == "list"]

    # Group the list rows by index and get the minimum price for each group
    min_lists = list_rows.groupby(list_rows.index)["price"].min()

    # Fill the lowest_listing_data dataframe with the minimum list for each index
    lowest_listing_data["lowest_listing"] = min_lists.reindex(lowest_listing_data.index).fillna(method="ffill")

    # Return the lowest_listing_data dataframe
    return lowest_listing_data


def get_trade_price(data: pd.DataFrame) -> pd.DataFrame:
    # Create a new DataFrame with the same index as the original data
    trade_price_data = pd.DataFrame(index=data.index, columns=["trade_price"])

    # Create a mask to filter out rows that are not "buyNow" type
    mask = data["type"] == "buyNow"

    # Use numpy's where function to assign the last trade price to the corresponding rows
    trade_price_data["trade_price"] = np.where(mask, data["price"], np.nan)
    trade_price_data["trade_price"] = trade_price_data["trade_price"].ffill()

    # Return trade_price_data DataFrame
    return trade_price_data


def calculate_metrics(data: DataFrame) -> DataFrame:
    # Get data.
    volatility_data: DataFrame = get_volatility(data=data)
    best_bid_data: DataFrame = get_best_bid(data=data)
    lowest_listing_data: DataFrame = get_lowest_listing(data=data)
    trade_price_data: DataFrame = get_trade_price(data=data)

    # Combine data, resample index, and drop NA
    combined_data: DataFrame = pd.concat(
        [volatility_data, best_bid_data, lowest_listing_data, trade_price_data], axis=1
    )
    combined_data: DataFrame = combined_data.resample("1S").last()
    combined_data: DataFrame = combined_data.dropna(how="all")

    # Return combined_data DataFrame
    return combined_data


def format_log_entry(data):
    log_entry: str = (
        f"Timestamp: {data.name.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Best Bid: {data['best_bid']:.2f}\n"
        f"Lowest Listing: {data['lowest_listing']:.2f}\n"
        f"Trade Price: {data['trade_price']:.2f}\n"
        f"1h Volatility: {data['volatility_1h']:.2f}\n"
        f"6h Volatility: {data['volatility_6h']:.2f}\n"
        f"24h Volatility: {data['volatility_24h']:.2f}"
    )
    return log_entry


def log_calculated_metrics(data: DataFrame, file_name: str = "calculated_aggregated_metrics.log") -> None:
    # Create a Series of formatted log entries
    log_entries: Series = data.apply(format_log_entry, axis=1)

    # Open the log file in append mode and write the entries
    with open(file_name, "w") as log_file:
        log_file.write(log_entries.str.cat(sep="\n"))
