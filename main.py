# !/usr/bin/env python3
# -*- coding: utf-8 -*-

from agent.agent import BaseAgent
from env.replay import Backtest

import numpy as np
import pandas as pd


class Agent(BaseAgent):

    def __init__(self, name, *args, **kwargs):
        """
        Trading agent implementation.

        The backtest iterates over a set of sources and alerts the trading agent
        whenever a source is updated. These updates are conveyed through method
        calls to, all of which you are expected to implement yourself: 

        - on_quote(self, market_id, book_state)
        - on_trade(self, market_id, trade_state)
        - on_time(self, timestamp, timestamp_next)
        
        In order to interact with a market, the trading agent needs to use the 
        market_interface instance available at `self.market_interface` that 
        provides the following methods to create and delete orders waiting to 
        be executed against the respective market's order book: 

        - submit_order(self, market_id, side, quantity, limit=None)
        - cancel_order(self, order)

        Besides, the market_interface implements a set of attributes that may 
        be used to monitor trading agent performance: 

        - exposure (per market)
        - pnl_realized (per market)
        - pnl_unrealized (per market)
        - exposure_total
        - pnl_realized_total
        - pnl_unrealized_total
        - exposure_left
        - transaction_costs

        The agent may also access attributes of related class instances, using 
        the container attributes: 

        - order_list -> [<Order>, *]
        - trade_list -> [<Trade>, *]
        - market_state_list -> {<market_id>: <Market>, *}

        For more information, you may list all attributes and methods as well
        as access the docstrings available in the base class using
        `dir(BaseAgent)` and `help(BaseAgent.<method>)`, respectively.

        :param name:
            str, agent name
        """
        super(Agent, self).__init__(name, *args, **kwargs)

        # TODO: YOUR IMPLEMENTATION GOES HERE

    def on_quote(self, market_id:str, book_state:pd.Series):
        """
        This method is called after a new quote.

        :param market_id:
            str, market identifier
        :param book_state:
            pd.Series, including timestamp, bid/ask price/quantity for 10 levels
        """

        # TODO: YOUR IMPLEMENTATION GOES HERE
        pass

    def on_trade(self, market_id:str, trades_state:pd.Series):
        """
        This method is called after a new trade.

        :param market_id:
            str, market identifier
        :param trades_state:
            pd.Series, including timestamp, price, quantity
        """

        # TODO: YOUR IMPLEMENTATION GOES HERE
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

        # TODO: YOUR IMPLEMENTATION GOES HERE
        pass


if __name__ == "__main__":

    # TODO: INSTANTIATE AGENT. Please refer to the corresponding file for more information.
    agent = Agent(
        name="test_agent",
        exposure_limit=1_000_000,  # exposure limit of EUR 1 million
        latency=10,  # latency in us (microseconds)
        transaction_cost_factor=5e-5,  # 0.5 bps variable transaction cost
        # ...
    )

    # TODO: INSTANTIATE BACKTEST. Please refer to the corresponding file for more information.
    backtest = Backtest(
        agent=agent,
    )

    # TODO: SELECT SOURCES. You may delete or comment out the rest.
    identifier_list = [
        # ADIDAS
        "Adidas.BOOK", "Adidas.TRADES",
        # ALLIANZ
        "Allianz.BOOK", "Allianz.TRADES",
        # BASF
        "BASF.BOOK", "BASF.TRADES",
        # Bayer
        "Bayer.BOOK", "Bayer.TRADES",
        # BMW
        "BMW.BOOK", "BMW.TRADES",
        # Continental
        "Continental.BOOK", "Continental.TRADES",
        # Covestro
        "Covestro.BOOK", "Covestro.TRADES",
        # Daimler
        "Daimler.BOOK", "Daimler.TRADES",
        # Deutsche Bank
        "DeutscheBank.BOOK", "DeutscheBank.TRADES",
        # DeutscheBörse
        "DeutscheBoerse.BOOK", "DeutscheBoerse.TRADES",
    ]

    # TODO: RUN BACKTEST. Please refer to the corresponding file for more information.
    # Option 1: run agent against a series of generated episodes, that is, 
    # generate episodes with the same episode_buffer and episode_length
    backtest.run_episode_generator(
        identifier_list=identifier_list,
        source_directory=r"C:\Users\Tino\Data\EFN2_MarketData_Part1",
        date_start="2021-01-02",
        date_end="2021-02-28", 
        episode_interval=10,
        episode_shuffle=False,
        episode_buffer=5,
        episode_length=5,
        num_episodes=10,
        seed=1337,
    )

    # Option 2: run agent against a series of specified episodes, that is,
    # list a tuple (episode_start_buffer, episode_start, episode_end) for each 
    # episode
    backtest.run_episode_list(
        identifier_list=identifier_list,
        source_directory=r"C:\Users\Tino\Data\EFN2",
        episode_list=[
            ("2021-01-04T08:00:00", "2021-01-04T08:05:00", "2021-01-04T08:06:00"),
            ("2021-01-04T15:00:00", "2021-01-04T15:05:00", "2021-01-04T15:06:00"),
            # ... 
        ],
    )

    # Option 3: run agent against a series of broadcast episodes, that is,
    # broadcast the same timestamps for every date between date_start and
    # date_end
    backtest.run_episode_broadcast(
        identifier_list=identifier_list,
        source_directory=r"C:\Users\Data\EFN II",
        date_start="2021-01-02",
        date_end="2021-01-07",
        time_start_buffer="08:00:00",
        time_start="08:05:00",
        time_end="08:06:00",
    )

    # TODO: EVALUATE YOUR BACKTESTING RESULTS
    # E.g., you can use result list backtest.results to analyze your runs
    pnl_realized_all_runs = [d['PnL_realized'] for d in backtest.results]
    pnl_realized_all_runs = pd.DataFrame(pnl_realized_all_runs)
    pnl_realized_all_runs.to_csv('PnL_realized_all_runs.csv', index_label='Run')
