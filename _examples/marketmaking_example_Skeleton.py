# !/usr/bin/env python3
# -*- coding: utf-8 -*-

from agent.agent import BaseAgent
from env.replay import Backtest

import numpy as np
import pandas as pd

class Agent(BaseAgent):

    def __init__(self, name, q, *args, **kwargs):
        """
        :param name:
            str, agent name
        :param q:
            int, fixed quantity of quotes
        
        Goal:
        
        This market making agent follows a simple principle: Always quote at 
        the top best price levels on bid and ask with fixed quantity. If a 
        a better price is offered on either side, we adjust our quotes.
        If one of our orders is fully executed, we submit a new order.
        
        """
        super(Agent, self).__init__(name, *args, **kwargs)
        self.q = q
        
        # Init a dict to save best bid and ask prices for each stock
        self.market_quotes = {}

    def on_quote(self, market_id:str, book_state:pd.Series):
        """
        This method is called after a new quote.

        :param market_id:
            str, market identifier
        :param book_state:
            pd.Series, including timestamp, bid/ask price/quantity for 10 levels
        """
        
        # Continously check market best bid and ask quote
        self.market_quotes[market_id] = {
            ...
        }                        

        # Check status of own orders
        
        # (1) Bid
        # Check whether we have active bid orders
        
        # Cancel existing order that is worse than best market bid
        
        # Submit if we have no active order (anymore)
 
        
        # (2) Repeat for Ask

                        
            
    def on_trade(self, market_id:str, trades_state:pd.Series):
        """
        This method is called after a new trade.

        :param market_id:
            str, market identifier
        :param trades_state:
            pd.Series, including timestamp, price, quantity
        """

        pass
    
    
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
        name="market_making",
        q=50,
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
        #"Covestro.BOOK", "Covestro.TRADES",
        # # Daimler
        # "Daimler.BOOK", "Daimler.TRADES",
        # # Deutsche Bank
        # "DeutscheBank.BOOK", "DeutscheBank.TRADES",
        # DeutscheBörse
        "DeutscheBoerse.BOOK", "DeutscheBoerse.TRADES",
    ]

    # Option 1: run agent against a single episode defined by start and end date
    #backtest.run_episode_list(
    #    identifier_list=identifier_list,
    #    source_directory=r'C:\Users\Tino\Data\EFN2_MarketData_Part1',
    #    episode_list=[
    #        ("2021-01-04T08:00:00", "2021-01-04T08:15:00", "2021-01-04T08:25:00"),
    #        #("2021-01-04T08:00:00", "2021-01-04T08:15:00", "2021-01-04T08:25:00"),
            # ...
    #    ],
    #)

    # Option 2: run agent against a series of generated episodes, that is,
    # generate episodes with the same episode_buffer and episode_length
    backtest.run_episode_generator(
        identifier_list=identifier_list,
        source_directory=r"C:\Users\Tino\Data\EFN2_MarketData_Part1",
        date_start="2021-01-02",
        date_end="2021-02-28",
        episode_interval=20,
        episode_shuffle=True,
        episode_buffer=5,
        episode_length=15,
        num_episodes=2,
        seed=123,
    )

    # E.g., you can use result list backtest.results to analyze your runs
    pnl_realized_all_runs = [d['PnL_realized'] for d in backtest.results]
    pnl_realized_all_runs = pd.DataFrame(pnl_realized_all_runs)
    pnl_realized_all_runs.to_csv('PnL_realized_Market_Making.csv', index_label='Run')