import multiprocessing
import time

import pandas as pd
from memory_profiler import memory_usage
from optimised import calc_pnl as optimised_calc_pnl
from original import calc_pnl as original_calc_pnl
from pandas import DataFrame

multiprocessing.set_start_method("fork")

data: DataFrame = pd.read_excel("./data_2.xlsx")
wallet_address: str = "Our Address"

# Original code
t1 = time.time()
original_mem_usage_stats = memory_usage((original_calc_pnl, (data, wallet_address)))
elapsed1 = time.time() - t1
original_mem_usage = max(original_mem_usage_stats) - min(original_mem_usage_stats)
print(f"Original code: Memory usage: {original_mem_usage} MB.")
print(f"Original code: Elapsed time: {elapsed1:.8f} seconds.")

# Optimised code
t2 = time.time()
optimised_mem_usage_stats = memory_usage((optimised_calc_pnl, (data, wallet_address)))
elapsed2 = time.time() - t2
optimised_mem_usage = max(optimised_mem_usage_stats) - min(optimised_mem_usage_stats)
print(f"Original code: Memory usage: {optimised_mem_usage} MB.")
print(f"Optimised code: Elapsed time: {elapsed2:.8f} seconds.")

# Compare time complexity
if elapsed1 < elapsed2:
    print(
        f"Optimised version is SLOWER by: {(elapsed2 - elapsed1):.8f} seconds "
        f"({(((elapsed2 - elapsed1) / elapsed2) * 100.0):.2f} %)."
    )
else:
    print(
        f"Optimised version is FASTER by: {(elapsed1 - elapsed2):.8f} seconds "
        f"({(((elapsed1 - elapsed2) / elapsed1) * 100.0):.2f} %)."
    )

# Compare space complexity
if original_mem_usage < optimised_mem_usage:
    print(
        f"Optimised version use LESS memory by: {(optimised_mem_usage - original_mem_usage):.8f} MB "
        f"({(((optimised_mem_usage - original_mem_usage) / optimised_mem_usage) * 100.0):.2f} %)."
    )
else:
    print(
        f"Optimised version is MORE memory by: {(original_mem_usage - optimised_mem_usage):.8f} MB "
        f"({(((original_mem_usage - optimised_mem_usage) / original_mem_usage) * 100.0):.2f} %)."
    )

# Results:
# Original code: Memory usage: 0.015625 MB.
# Original code: Elapsed time: 0.21818900 seconds.
# Original code: Memory usage: 0.0625 MB.
# Optimised code: Elapsed time: 0.16550708 seconds.
# Optimised version is FASTER by: 0.05268192 seconds (24.15 %).
# Optimised version use LESS memory by: 0.04687500 MB (75.00 %).
