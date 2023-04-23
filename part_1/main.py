import pandas as pd
from compute import calculate_metrics, log_calculated_metrics
from pandas import DataFrame
from strategy import TradingStrategy

print("Loading data ...")
data: DataFrame = pd.read_csv("./data_1.csv")
data["blockTime"] = pd.to_datetime(data["blockTime"], unit="s")
data: DataFrame = data.set_index("blockTime")
data: DataFrame = data.sort_index(ascending=True)

print("Calculating metrics ...")
calculated_metrics: DataFrame = calculate_metrics(data=data)

print("Storing metrics ...")
log_calculated_metrics(data=calculated_metrics)

print("Backtesting trading strategy ...")
TradingStrategy(metrics=calculated_metrics).backtest()

print("DONE")
