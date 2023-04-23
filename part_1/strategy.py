import numpy as np
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm


class TradingStrategy:
    def __init__(
        self,
        metrics: DataFrame,
        start_cash: float = 1_000.0,
        log_file_name: str = "./trading_history.log",
        min_trade_gain: float = 1.2,
    ) -> None:
        self._metrics: DataFrame = metrics
        self._cash: float = start_cash
        self._log_file_name: str = log_file_name
        self._pnl: DataFrame = DataFrame()
        self._min_trade_gain: float = min_trade_gain  # We want at least e.g. 3% / trade.

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def pnl(self) -> DataFrame:
        return self._pnl

    def _get_pnl(self) -> float:
        return self._pnl["pnl"].sum()

    def _get_sharpe_ratio(self) -> float:
        # TODO: Note that the Sharpe ratio assumes that returns are normally distributed.
        # Resample the data to daily frequency and sum up the PnL for each day
        daily_pnl = self._pnl["pnl"].resample("D").sum()

        # Calculate the daily returns
        daily_returns = daily_pnl.pct_change()

        # Calculate the daily standard deviation of returns
        daily_std = np.std(daily_returns)

        # Calculate the annualized Sharpe ratio
        annualized_sharpe_ratio = np.sqrt(365) * (daily_returns.mean() / daily_std)

        return annualized_sharpe_ratio

    def _get_stats(self) -> None:
        pnl_value: float = self._get_pnl()
        sharpe_ratio: float = self._get_sharpe_ratio()

        print(f"PnL: {pnl_value:.2f}\n" f"Sharpe Ratio: {sharpe_ratio:.2f}\n")

    def backtest(self) -> None:
        have_position: bool = False
        last_buy_price: float = 0.0

        pln_data: list = []

        with open(self._log_file_name, "w") as log_file:
            for idx, row in tqdm(self._metrics.iterrows(), total=self._metrics.shape[0]):

                if have_position:
                    # Waiting for the opportunity to SELL

                    # SELL only where someone realistically sold - to make backtest a bit more realistic
                    if pd.notna(row["trade_price"]):

                        # TODO: For now expect that we can sell once there was a "buyNow" event. Cons we do not know
                        #  if it was a floor-level item or any higher value item
                        if row["lowest_listing"] > last_buy_price * self._min_trade_gain:
                            # Selling position now (= someone bought a NFT)
                            self._cash += row["lowest_listing"]
                            # TODO: fees are not taken into consideration here
                            pln_data.append({"timestamp": row.name, "pnl": row["lowest_listing"] - last_buy_price})

                            # Log
                            log_file.write(
                                f"Timestamp: {row.name}\n" f"Sell Signal\n" f"Trade Price: {row['lowest_listing']}\n"
                            )

                            have_position: bool = False

                else:
                    # Waiting for the opportunity to BUY

                    # Will buy only if best_bid is close to lowest_listing enough
                    if row["best_bid"] > row["lowest_listing"] * 0.95:
                        # Buying position now (= bought a NFT)
                        last_buy_price: float = row["lowest_listing"]
                        self._cash -= row["lowest_listing"]

                        # Log
                        log_file.write(
                            f"Timestamp: {row.name}\n" f"Buy Signal\n" f"Trade Price: {row['lowest_listing']}\n"
                        )

                        if self._cash < 0:
                            print(f"Warning: cash is negative: {self._cash}")

                        have_position: bool = True

        # Transform PnL data.
        self._pnl: DataFrame = pd.DataFrame(pln_data)
        self._pnl: DataFrame = self._pnl.set_index("timestamp")

        # Print stats
        self._get_stats()
