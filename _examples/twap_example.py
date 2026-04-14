# !/usr/bin/env python3
# -*- coding: utf-8 -*-

from agent.agent import BaseAgent
from env.replay import Backtest

import numpy as np
import pandas as pd


class TwapAgent(BaseAgent):
    """
    This TWAP (Time-Weighted Average Price) agent follows a simple execution principle:
    The goal is to spread execution uniformly over a defined timeperiod, reducing market 
    impact by avoiding a single large order.
    The large parent order is split into a fixed number of equally-sized child orders.
    The child orders are submitted at evenly spaced intervals between a given start and end time. 
    For this, the agent can create a schedule with timestamps of planned order submissions.
    On each time step, the agent checks how many schedule slots have elapsed and submits 
    any outstanding child orders as market order.
    """

    def __init__(self, name: str, 
                 parent_order_quantity: float,
                 num_child_orders: int, 
                 algo_start_time: str,
                 algo_end_time: str,):
        """
        Trading agent implementation example. Improved version.

        :param name:
            str, agent name
        :param parent_order_quantity:
            float, total quantity to be executed
        :param num_child_orders:
            int, number of equally-sized child orders to split the parent order into
        :param algo_start_time:
            str, start time of the execution window (e.g. "10:00")
        :param algo_end_time:
            str, end time of the execution window (e.g. "15:00")
        """

        super(TwapAgent, self).__init__(name)

        # static attributes from arguments
        self.name = name
        self.parent_quantity = parent_order_quantity
        self.num_child_orders = num_child_orders
        self.algo_start_time = algo_start_time
        self.algo_end_time = algo_end_time

        # compute quantity of child orders
        self.child_qty = self.parent_quantity / self.num_child_orders

        # set schedule to None initially
        self.schedule = None
        # set order counter to 0 initially
        self.orders_sent = 0

    def on_quote(self, market_id:str, book_state:pd.Series):
        """
        This method is called after a new quote.

        :param market_id:
            str, market identifier
        :param book_state:
            pd.Series, including timestamp, bid/ask price/size for 10 levels
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
        
        # Build the execution schedule on the very first on_time call.
        # We extract the date from the
        # live timestamp and combine it with the argument-supplied start/end times
        # to get full Timestamps. pd.date_range with periods=N then places
        # exactly N equally-spaced points between start and end.

        # Check if self.schedule is None
        if self.schedule is None:
            # Obtain date from timestamp
            date = timestamp.date()
            # Create start and end timestamps combining date and clock times
            start = pd.Timestamp(f"{date} {self.algo_start_time}")
            end   = pd.Timestamp(f"{date} {self.algo_end_time}")
            # generates N evenly spaced timestamps between start and end (both inclusive).
            self.schedule = pd.date_range(start, end, periods=self.num_child_orders)
            print(self.schedule)

        # Count how many schedule slots have elapsed up to and including now.
        # Whenever that number exceeds the orders already sent, we are behind
        # schedule and fire one order to catch up.
        slots_due = sum(1 for t in self.schedule if t <= timestamp)
        while self.orders_sent < slots_due:
            # Submit market order
            self.market_interface.submit_order("Allianz", "buy", self.child_qty)
            # Count child order submission
            self.orders_sent += 1


if __name__ == "__main__":

    identifier_list = [
        "Allianz.BOOK", "Allianz.TRADES"
    ]

    agent = TwapAgent(
        name = "twap1", 
        parent_order_quantity = 1000,
        num_child_orders = 10, 
        algo_start_time = "10:00",
        algo_end_time = "15:00")
    
    backtest = Backtest(
        agent=agent, 
    )

    # Option 3: run agent against a series of specified episodes, that is, 
    # list a tuple (episode_start_buffer, episode_start, episode_end) for each 
    # episode
    backtest.run_episode_list(
        identifier_list=identifier_list,
        source_directory = "/Users/ewald/Data/EFN2_PART1",
        episode_list=[
            ("2021-01-04T08:00:00", "2021-01-04T08:20:00", "2021-01-04T16:30:00"),
        ],
    )
