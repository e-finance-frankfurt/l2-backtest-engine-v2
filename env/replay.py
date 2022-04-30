# !/usr/bin/env python3
# -*- coding: utf-8 -*-

# TODO: add seed
# TODO: ...

# ...
from .market import MarketState, Order, Trade
from ..agent.agent import Agent

# ...
import datetime
import logging
import os
import pandas as pd
import random
random.seed(42)
import time

from concurrent import futures

SOURCE_DIRECTORY = "..."
DATETIME = "TIMESTAMP_UTC"

class Episode:

    def __init__(self,
        identifier_list:list,
        episode_start_buffer:pd.Timestamp,
        episode_start:pd.Timestamp,
        episode_end:pd.Timestamp,
    ):
        """
        Prepare a single episode as a generator. The episode is the main 
        building block of each backtest.

        :param episode_start_buffer:
            pd.Timestamp, ...
        :param episode_start:
            pd.Timestamp, ...
        :param episode_end:
            pd.Timestamp, ...
        """

        # data settings
        self.identifier_list = identifier_list

        # ...
        self._episode_start_buffer = pd.Timestamp(episode_start_buffer)
        self._episode_start = pd.Timestamp(episode_start)
        self._episode_end = pd.Timestamp(episode_end)

        # setup routine
        self._episode_setup(
            tolerance=300, # in seconds
        ) 

        # dynamically set attributes (on per-update basis)
        self._episode_buffering = None

    # static attributes ---

    @property
    def episode_start_buffer(self): 
        return self._episode_start_buffer 
    
    @property
    def episode_start(self):
        return self._episode_start 

    @property
    def episode_end(self):
        return self._episode_end

    # dynamic attributes ---

    @property
    def episode_buffering(self):
        return self._episode_buffering

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def timestamp_next(self):
        return self._timestamp_next

    # episode setup ---

    # DONE
    def _episode_setup(self, tolerance=300): 
        """
        Load and prepare episode for the  

        :param timestamp_start:
            pd.Timestamp/None, set episode starting from this timestamp, set random timestamp if None
        """

        # display progress ---

        # info
        logging.info("(INFO) episode from {timestamp_start} ({timestamp_buffer}) to {timestamp_end} is being prepared ...".format(
            episode_start=self._episode_start,
            episode_start_buffer=self._episode_start_buffer.time(),
            episode_end=self._episode_end,
        ))

        # in the beginning, unset flag to disallow iteration via __iter__ method
        self._episode_available = False

        # prepare data ---

        # build path_store to host all paths to load data from (only for this particular episode)
        path_store = self._build_path_store(self._episode_start_buffer, self._episode_end)
        # build data_store to host all data (only for this particular episode)
        data_store = self._build_data_store(self._episode_start_buffer, self._episode_end, path_store)
        # align data_store so that each data source has equal length
        data_store = self._align_data_store(data_store)
        # build data_monitor to iterate over
        data_monitor = self._build_data_monitor(data_store)
        
        # set attributes ---

        # set data_store to iterate over using the __iter__ method
        self._data_store = data_store
        # set data_monitor to iterate over using the __iter__ method
        self._data_monitor = data_monitor

        # sanity check ---

        # total time_delta should not deviate from episode_length by more than <tolerance> seconds
        time_delta_observed = self._data_monitor.iloc[-1, 0] - self._data_monitor.iloc[0, 0]
        time_delta_required = pd.Timedelta(tolerance, "s")
       
        # ... 
        assert time_delta_observed < time_delta_required, \
            "(ERROR) time delta exceeded tolerance (required: {required}, observed: {observed})".format(
                required=time_delta_required, 
                observed=time_delta_observed, 
            )
        
        # allow iteration ---

        # set flag to allow iteration via __iter__method
        self._episode_available = True

        # info
        logging.info("(INFO) episode has successfully been set and includes a total of {num_steps} steps".format(
            num_steps=len(data_monitor.index),
        ))

    # helper methods ---

    def _build_path_store(self, timestamp_start, timestamp_end):
        """
        Find paths and store them in the path_store dictionary together with 
        their corresponding key.
        
        Note that timestamp_start and timestamp_end must belong to the same 
        date!

        :param timestamp_start:
            pd.Timestamp, ...
        :param timestamp_end:
            pd.Timestamp, ...

        :return path_store:
            dict, {<identifier>: <path>, *}
        """

        path_store = dict()

        # ...
        assert timestamp_start.date() == timestamp_end.date(), \
            "(ERROR) timestamp_start and timestamp_end must belong to the same date"
        date = timestamp_start.date()
        date_string = str(date).replace("-", "")

        # path_list includes all paths available in directory
        path_list = [os.path.join(pre, file) for pre, _, sub in os.walk(self.directory) 
            for file in sub if not file.startswith((".", "_"))
        ]

        # ...
        for identifier in self.identifier_list:

            # identify matching criteria
            market_id, event_id = identifier.split(".")

            # make copy of path_list
            path_list_ = path_list.copy()
            # filter based on matching criteria
            path_list_ = filter(
                lambda path: market_id.lower() in path.lower(), path_list_)
            path_list_ = filter(
                lambda path: event_id.lower() in path.lower(), path_list_)
            path_list_ = filter(
                lambda path: date_string in path, path_list_)
            # ...
            path_list_ = list(path_list_)

            # if path_list_this is empty, raise Exception that is caught in calling method
            if not len(path_list_) == 1:
                raise Exception("(ERROR) could not find path for {identifier} between {timestamp_start} and {timestamp_end}".format(
                    identifier=identifier,
                    timestamp_start=timestamp_start, timestamp_end=timestamp_end,
                ))

            # there should be exactly one matching path
            path = path_list_[0]

            # add dataframe to output dictionary
            path_store[identifier] = path

        # info
        logging.info("(INFO) path_store has been built")

        return path_store

    def _build_data_store(self, timestamp_start, timestamp_end, path_store):
        """
        Load .csv(.gz) and .json files into dataframes and store them in the
        data_store dictionary together with their corresponding key.

        :param path_store:
            dict, {<identifier>: <path>, *}
        :param timestamp_start:
            pd.Timestamp, ...
        :param timestamp_start:
            pd.Timestamp, ...

        :return data_store:
            dict, {<identifier>: <pd.DataFrame>, *}, original timestamps
        """

        data_store = dict()

        # ...
        for identifier in self.identifier_list:

            # load event_id 'BOOK' as .csv(.gz)
            if "BOOK" in identifier:
                df = pd.read_csv(path_store[identifier], parse_dates=[DATETIME])
            # load event_id 'TRADES' as .json
            if "TRADES" in identifier:
                df = pd.read_json(path_store[identifier], convert_dates=True)

            # if dataframe is empty, raise Exception that is caught in calling method
            if not len(df.index) > 0:
                raise Exception("(ERROR) could not find data for {identifier} between {timestamp_start} and {timestamp_end}".format(
                    identifier=identifier,
                    timestamp_start=timestamp_start, timestamp_end=timestamp_end,
                ))

            # filter dataframe to include only rows with timestamp between timestamp_start and timestamp_end
            df = df[df[DATETIME].between(timestamp_start, timestamp_end)] 
            # make timestamp timezone-unaware
            df[DATETIME] = pd.DatetimeIndex(df[DATETIME]).tz_localize(None)

            # add dataframe to output dictionary
            data_store[identifier] = df

        # info
        logging.info("(INFO) data_store has been built")

        return data_store

    def _align_data_store(self, data_store):
        """
        Consolidate and split again all sources so that each source dataframe
        contains a state for each ocurring timestamp across all sources.

        :param data_store:
            dict, {<identifier>: <pd.DataFrame>, *}, original timestamps

        :return data_store:
            dict, {<identifier>: <pd.DataFrame>, *}, aligned timestamps
        """

        # unpack dictionary
        id_list, df_list = zip(*data_store.items())

        # rename columns and use id as prefix, exclude timestamp
        add_prefix = lambda id, df: df.rename(columns={x: f"{id}__{x}"
            for x in df.columns[1:]
        })
        df_list = list(map(add_prefix, id_list, df_list))

        # join df_list into df_merged (full outer join)
        df_merged = pd.concat([
            df.set_index(DATETIME) for df in df_list
        ], axis=1, join="outer").reset_index()

        # split df_merged into original df_list (all df are of equal length)
        df_list = [pd.concat([
            df_merged[[DATETIME]], # global timestamp
            df_merged[[x for x in df_merged.columns if id in x] # filtered by identifier
        ]], axis=1) for id in id_list]

        # rename columns and remove prefix, exclude timestamp
        del_prefix = lambda df: df.rename(columns={x: x.split("__")[1]
            for x in df.columns[1:]
        })
        df_list = list(map(del_prefix, df_list))

        # pack dictionary
        data_store = dict(zip(id_list, df_list))

        # info
        logging.info("(INFO) data_store has been aligned")

        return data_store

    def _build_data_monitor(self, data_store):
        """
        In addition to the sources dict, return a monitor dataframe that keeps
        track of changes in state across all sources.

        :param data_store:
            dict, {<source_id>: <pd.DataFrame>, *}, aligned timestamp

        :return data_monitor:
            pd.DataFrame, changes per source and timestamp
        """

        # setup dictionary based on timestamp
        datetime_index = list(data_store.values())[0][DATETIME]
        data_monitor = {DATETIME: datetime_index}

        # track changes per source and timestamp in series
        for key, df in data_store.items():
            data_monitor[key] = ~ df.iloc[:, 1:].isna().all(axis=1)

        # build monitor as dataframe from series
        data_monitor = pd.DataFrame(data_monitor)

        # info
        logging.info("(INFO) data_monitor has been built")

        return data_monitor

    # iteration ---
        
    def __next__(self):
        pass

    def __iter__(self):
        """
        Iterate over the set episode. 
        
        NOTE: Use the self._episode_buffering flag to check if the buffering 
        phase has ended - only then should the agent be notified about market 
        updates. 
        """
        
        # return, that is, disallow iteration if no episode has been set
        if not self._episode_available:
            return
        
        # ...
        logging.info("(INFO) episode has started ...")
        
        # set buffer flag
        self._episode_buffering = True

        # time
        time_start = time.time()

        # ...
        for step, timestamp, *monitor_state in self._data_monitor.itertuples():

            # update timestamps ---

            # track this timestamp
            self._timestamp = self._data_monitor.iloc[step, 0]
            
            # track next timestamp, prevent IndexError that would arise with the last step
            self._timestamp_next = self._data_monitor.iloc[min(
                step + 1, len(self._data_monitor.index)
            ), 0]

            # display progress ---

            # ...
            progress = timestamp.value / (self._episode_end.value - self._episode_start_buffer.value)
            eta = (time.time() - time_start) / progress

            # info
            logging.info("(INFO) step {step}, progress {progress}, eta {eta}, time {timestamp}".format(
                step=step,
                progress=progress,
                eta=eta,
            ))

            # handle buffer phase ---
            
            # update buffer flag, agent should start being informed only after buffering phase has ended
            cache_episode_buffering = self._episode_buffering
            self._episode_buffering = timestamp < self._episode_start
            
            # info
            if cache_episode_buffering != self._episode_buffering:
                logging.info("(INFO) buffering phase for this episode has ended, allow trading ...")
            
            # find update data ---

            # get identifier (column name) per updated source (based on self._data_monitor)
            identifier_list = (self._data_monitor
                .iloc[:, 1:]
                .columns[monitor_state]
                .values
            )

            # get data per updated source (based on self._data_store)
            data_list = [self._data_store[identifier].iloc[step, :] 
                for identifier in identifier_list
            ]

            # yield update data ---

            # for each step, yield update via dictionary
            update = zip(identifier_list, data_list) # {<identifier>: <data>, *}

            # ...
            yield update
        
        # time
        time_end = time.time()
        
        # ...
        time_delta = round(time_end - time_start, 3)
        time_per_step = round((time_end - time_start) / step, 3)

        # info
        logging.info("(INFO)... episode has ended, took {time_delta}s for {step} steps ({time_per_step}s/step)".format(
            time_delta=time_delta,
            step=step,
            time_per_step=time_per_step,
        ))

class Backtest:

    timestamp_global = None

    def __init__(self,
        agent:Agent, # backtest is wrapper for trading agent
    ):
        """
        Backtest wrapper that is used to evaluate a trading Agent on one or 
        multiple episodes of historical market data. 

        :param agent:
            Agent, trading agent instance that is to be evaluated
        """

        # from arguments
        self.agent = agent

        # dynamically set (episode)
        self.episode = None

        # statically referenced (market)
        self.market_state_list = MarketState.instances
        self.order_list = Order.history
        self.trade_list = Trade.history

        # ...
        self.result_list = []    

    # market/agent step ---

    def _market_step(self, market_id, book_state, trades_state):
        """
        Update post-trade market state and match standing orders against 
        pre-trade market state.

        :param market_id:
            str, market identifier
        :param book_state:
            pd.Series, ...
        :param trades_state:
            pd.Series, ...
        """

        # update market state
        self.market_state_list[market_id].update(
            book_state=book_state,
            trades_state=trades_state,
        )

        # match standing agent orders against pre-trade state
        self.market_state_list[market_id].match()

    def _agent_step(self, source_id, either_state, timestamp, timestamp_next):
        """
        Inform trading agent about either book or trades state through the 
        corresponding method. Also, inform trading agent about this and next 
        timestamp. 

        :param source_id:
            str, source identifier
        :param either_state:
            pd.Series, ...
        :param timestamp:
            pd.Timestamp, ...
        :param timestamp_next:
            pd.Timestamp, ...
        """

        # case 1: alert agent every time that book is updated
        if source_id.endswith("BOOK"):
            self.agent.on_quote(market_id=source_id.split(".")[0], 
                book_state=either_state,
            )
        # case 2: alert agent every time that trade happens
        elif source_id.endswith("TRADES"):
            self.agent.on_trade(market_id=source_id.split(".")[0],
                trades_state=either_state,
            )
        # unknown source_id
        else:
            raise Exception("(ERROR) unable to parse source_id '{source_id}'".format(
                source_id=source_id, 
            ))
        
        # _always_ alert agent with time interval between this and next timestamp
        self.agent.on_time(
            timestamp=timestamp,
            timestamp_next=timestamp_next,
        )

    # option 1: run ---

    def run(self, 
        identifier_list:list,
        episode_start_buffer:pd.Timestamp,
        episode_start:pd.Timestamp,
        episode_end:pd.Timestamp,
        display_interval:int=100,
    ):  
        """
        Run agent against a single backtest instance based on a specified 
        episode. 

        :param identifier_list:
            list, <market_id>.BOOK/TRADES identifier for each respective data source
        :param episode_start_buffer:
            pd.Timestamp, 
        :param episode_start:
            pd.Timestamp, ...
        :param episode_end:
            pd.Timestamp, ...
        """

        # build episode ---

        # try to build episode based on the specified parameters
        try: 
            episode = Episode(
                identifier_list=identifier_list,
                episode_start_buffer=episode_start_buffer,
                episode_start=episode_start,
                episode_end=episode_end,
            )
        # raise Exception if episode could not be generated
        except:
            raise Exception("(ERROR) could not run episode with the specified parameters")

        # build market environment ---

        # identify market instances based on market_id
        identifier_list = set(identifier.split(".")[0] for identifier
            in identifier_list
        )
        # setup market instances
        for market_id in identifier_list:
            _ = MarketState(market_id)

        # iterate over episode ---

        # ...
        for step, update_store in enumerate(episode, start=1): 
            
            # update global timestamp
            self.__class__.timestamp_global = episode.timestamp_this
            
            # ...
            market_list = set(identifier.split(".")[0] for identifier in update_store)
            source_list = list(update_store)

            # step 1: update book_state -> based on original data
            # step 2: match standing orders -> based on pre-trade state
            for market_id in market_list:
                self._market_step(market_id=market_id, 
                    book_state=update_store.get(f"{market_id}.BOOK"), 
                    trades_state=update_store.get(f"{market_id}.TRADES", pd.Series([])), # empty pd.Series
                )
            
            # during the buffer phase, do not inform agent about update
            if self.episode.episode_buffering:
                continue

            # step 3: inform agent -> based on original data
            for source_id in source_list: 
                self._agent_step(source_id=source_id, 
                    either_state=update_store.get(source_id),
                    timestamp=episode.timestamp,
                    timestamp_next=episode.timestamp_next,
                )

            # finally, report the current state of the agent
            if not (step % display_interval):
                print(self.agent.market_interface)
        
        # delete market environment ---

        # delete all MarketState instances in MarketState.instances class attribute
        MarketState.reset_instances()
        # delete all Order instances in Order.history class attribute
        Order.reset_history()
        # delete all Trade instances in Trade.history class attribute
        Trade.reset_history()

        # report outcome ---

        # ...
        result = None
        # save report
        self.result_list.append(result)

    # option 2: run_generator ---

    def run_generator(self,
        identifier_list:list,
        date_start:str="2016-01-01",
        date_end:str="2016-01-31",
        episode_interval:int=None, # timestamp quantization
        episode_shuffle:bool=True,
        episode_buffer:int=5,
        episode_length:int=30, 
        num_episodes:int=None,
    ):
        """
        Run agent against a series of backtest instances based on an episode 
        generator. Call Backtest.run(...) using multi-threading. 

        :param identifier_list:
            list, <market_id>.BOOK/TRADES identifier for each respective data source
        :param date_start:
            pd.Timestamp, start date after which episodes are generated, default is "2016-01-01"
        :param date_end:
            pd.Timestamp, end date before which episodes are generated, default is "2016-03-31"
        :param episode_interval:
            int, episode grid that defines available episode_start options (in minutes), default is 5
        :param episode_shuffle:
            bool, whether to shuffle episodes or not, default is True
        :param episode_buffer:
            int, length of buffer phase that is required to build up the market (in minutes), default is 5
        :param episode_length:
            int, length of episode _including_ the episode_buffer (in minutes), default is 30
        :param num_episodes:
            int, number of episodes to run as part of the backtest, default is None
        """

        # ...
        date_start = pd.Timestamp(date_start)
        date_end = pd.Timestamp(date_end)
        
        # ...
        episode_buffer = pd.Timedelta(episode_buffer, "min")
        episode_length = pd.Timedelta(episode_length, "min")

        # build episode grid ---

        # case 1: episode_interval is specified 
        if episode_interval:
            
            # build episode_grid
            episode_grid = pd.date_range(start=date_start, end=date_end, 
                freq=f"{episode_interval}m", 
                normalize=True,
            )
            # filter episode_grid with regard to business days, hours
            test_list = [
                lambda timestamp: timestamp.weekday() not in [5, 6], # sat, sun
                lambda timestamp: datetime.time(8, 0, 0) <= timestamp.time(), # valid start
                lambda timestamp: (timestamp + episode_length).time() <= datetime.time(16, 30, 0), # valid end
                # ...
            ]
        
        # case 2: episode_interval is not specified
        else:
            # ...
            episode_grid = pd.date_range(start=date_start, end=date_end + , 
                freq=f"{1}d", 
                normalize=True,
            )
            # filter episode_grid with regard to business days, hours
            test_list = [
                lambda timestamp: timestamp.weekday() not in [5, 6], # sat, sun
                # ...
            ]

        # filter episode_grid
        episode_grid = [timestamp_start for timestamp_start in episode_grid 
            if all(test(timestamp_start) for test in test_list)
        ]

        # shuffle episode grid 
        if episode_shuffle:
            random.seed(seed)
            episode_grid = random.shuffle(episode_grid)

        # iterate episode grid ---

        # ...
        episode_counter = 0
        episode_index = 0

        # ...
        while episode_counter < min(len(episode_grid), num_episodes): 
            
            # ...
            try:
                episode_start_buffer = episode_grid[episode_index]
                episode_start = episode_start_buffer + pd.Timedelta(episode_buffer, "min")
                episode_end = episode_start_buffer + pd.Timedelta(episode_length, "min")

                # run episode
                self.run(identifier_list=identifier_list,
                    episode_start_buffer=episode_start_buffer,
                    episode_start=episode_start,
                    episode_end=episode_end,
                )

                # update index
                episode_counter = episode_counter + 1
                episode_index = episode_index + 1

            # ...
            except:
                pass # skip
            
            # ...
            finally: 
                episode_index = episode_index + 1


