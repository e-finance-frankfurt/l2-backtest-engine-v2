# !/usr/bin/env python3
# -*- coding: utf-8 -*-

from agent.agent import BaseAgent
from env.replay import Backtest

import numpy as np
import pandas as pd

class Agent(BaseAgent):

    def __init__(self, name, p1, p2, *args, **kwargs):
        """
        :param name:
            str, agent name
        :param p1:
            float, probability of limit order submission
        :param p2:
            float, cond. probability of buy limit order submission
        """
        super(Agent, self).__init__(name, *args, **kwargs)
        self.p1 = p1
        self.p2 = p2

    def on_quote(self, market_id:str, book_state:pd.Series):
        """
        This method is called after a new quote.

        :param market_id:
            str, market identifier
        :param book_state:
            pd.Series, including timestamp, bid/ask price/quantity for 10 levels
        """

        pass

    def on_trade(self, market_id:str, trades_state:pd.Series):
        """
        This method is called after a new trade.

        :param market_id:
            str, market identifier
        :param trades_state:
            pd.Series, including timestamp, price, quantity
        """

        # continuously uniformly distributed random variable z1
        z1 = np.random.rand()

        if z1 < self.p1:

            # continuously uniformly distributed random variable z2
            z2 = np.random.rand()

            if z2 < self.p2:
                self.market_interface.submit_order(
                    market_id, 'buy', 100, self.market_interface.market_state_list[market_id].best_bid)
            else:
                self.market_interface.submit_order(
                    market_id, 'sell', 100, self.market_interface.market_state_list[market_id].best_ask)

    def on_time(self, timestamp:pd.Timestamp, timestamp_next:pd.Timestamp):
        """
        This method is called with every iteration and provides the timestamps
        for both current and next iteration. The given interval may be used to
        submit orders before a specific point in time.

        :param timestamp:
            pd.Timestamp, timestamp recorded
        :param timestamp_next:
            pd.Timestamp, timestamp recorded in next iteration
        """

        pass


if __name__ == "__main__":

    agent = Agent(
        name="intro_agent",
        p1=0.2,
        p2=0.75,
        exposure_limit=1_000_000,  # exposure limit of EUR 1 million
        latency=10,  # latency in us (microseconds)
        transaction_cost_factor=5e-5,  # 0.5 bps variable transaction cost
    )

    backtest = Backtest(
        agent=agent,
    )

    identifier_list = [
        # ADIDAS
        "Adidas.BOOK", "Adidas.TRADES",
        # # ALLIANZ
        # "Allianz.BOOK", "Allianz.TRADES",
        # # BASF
        # "BASF.BOOK", "BASF.TRADES",
        # # Bayer
        # "Bayer.BOOK", "Bayer.TRADES",
        # # BMW
        # "BMW.BOOK", "BMW.TRADES",
        # # Continental
        # "Continental.BOOK", "Continental.TRADES",
        # Covestro
        "Covestro.BOOK", "Covestro.TRADES",
        # # Daimler
        # "Daimler.BOOK", "Daimler.TRADES",
        # # Deutsche Bank
        # "DeutscheBank.BOOK", "DeutscheBank.TRADES",
        # # DeutscheBörse
        # "DeutscheBoerse.BOOK", "DeutscheBoerse.TRADES",
    ]

    # Option 1: run agent against a series of generated episodes, that is,
    # generate episodes with the same episode_buffer and episode_length
    backtest.run_episode_generator(
        identifier_list=identifier_list,
        source_directory=r"C:\Users\cesto\Data\EFN II",
        date_start="2021-01-02",
        date_end="2021-02-28", 
        episode_interval=20,
        episode_shuffle=False,
        episode_buffer=5,
        episode_length=15,
        num_episodes=2,
        seed=123,
    )

    # TODO: EVALUATE YOUR BACKTESTING RESULTS
    # E.g., you can use result list backtest.results to analyze your runs
    pnl_realized_all_runs = [d['PnL_realized'] for d in backtest.results]
    pnl_realized_all_runs = pd.DataFrame(pnl_realized_all_runs)
    pnl_realized_all_runs.to_csv('PnL_realized_all_runs.csv', index_label='Run')