# l2-backtest-engine-v2

This is a project from the chair of e-Finance at Goethe University Frankfurt.

# Documentation

## Write your custom trading agent

### 1. Create your agent
Write a class that inherits from the BaseAgent class. 

Use ```on_quote```, ```on_trade```, ```on_time``` methods to react on quotes, trades, and timestamps, respectively.

```python
class CustomAgent(BaseAgent):

    def __init__(self, name, *args, **kwargs):
        super(Agent, self).__init__(name, *args, **kwargs)

    def on_quote(self, market_id:str, book_state:pd.Series):
        pass

    def on_trade(self, market_id:str, trades_state:pd.Series):
        pass

    def on_time(self, timestamp:pd.Timestamp, timestamp_next:pd.Timestamp):
        pass
```

### 2. Customize your agent
In your ```CustomAgent``` class, use pre-defined methods to access and interact with available markets or to check current trading performance. 

#### Interacting with market

Submit an order:
```python
self.market_interface.submit_order(market_id, side, quantity, limit=None)
```

Delete an order: 
```python
self.market_interface.cancel_order(order_object)
```

#### Interacting with agent-generated trades and orders

List of all agent-generated trade objects:
```python
self.market_interface.trade_list
```

Filter list of all agent-generated trade objects:
```python
self.market_interface.get_filtered_trades(market_id=None, side=None)
```

List of all agent-generated order objects: 
```python
self.market_interface.order_list
```

Filter list of all agent-generated order objects:
```python
self.market_interface.get_filtered_orders(market_id=None, side=None, status=None)
```

#### Exposure metrics
Get total exposure:
```python
self.market_interface.exposure_total
```
Get dict of exposure per security:
```python
self.market_interface.exposure
```

Get total exposure limit:
```python
self.market_interface.exposure_limit
```

Get total exposure left:
```python
self.market_interface.exposure_left
```

#### Performance metrics

Get total realized PnL (closed positions only):
```python
self.market_interface.pnl_realized_total
```

Get dict of realized PnL per security (closed positions only):
```python
self.market_interface.pnl_realized
```

Get total unrealized PnL (open positions only):
```python
self.market_interface.pnl_unrealized_total
```

Get dict of unrealized PnL per security (open positions only)
```python
self.market_interface.pnl_unrealized
```

Get variable transaction cost per trade
```python
self.market_interface.transaction_cost_factor
```

Get total transaction costs
```python
self.market_interface.transaction_cost
```

Note: PnLs do not cover transaction cost!

### 3. Define backtest
Create an instance of your ```CustomAgent``` class and the ```Backtest``` class.

Example:
```python
my_agent = CustomAgent(
    name="test_agent",
    transaction_cost_factor=1e-4,  # 1 bps variable transaction cost
)

backtest = Backtest(
    agent=my_agent,
)
```

### 4. Run backtest

Use one of the three following methods from ```Backtest``` to run your backtest.


Method 1: run_episode_generator
```python
run_episode_generator(self, 
        identifier_list:list,
        source_directory: str,
        date_start:str,
        date_end:str,
        episode_interval:int=30,
        episode_shuffle:bool=True,
        episode_buffer:int=5,
        episode_length:int=30, 
        num_episodes:int=10,
        sampling_freq:int or str=1,
        seed=None,
    ):
```


Method 2: run_episode_list
```python
run_episode_list(self, 
        identifier_list:list,
        source_directory:str,
        episode_list:list,
        sampling_freq:int or str=1,

    ):
```


Method 3: run_episode_broadcast
```python
run_episode_broadcast(self, 
        identifier_list:list,
        source_directory:str,
        date_start:str,
        date_end:str,
        time_start_buffer:str="08:00:00",
        time_start:str="08:10:00", 
        time_end:str="16:30:00",
        sampling_freq:int or str=1,
):
```

### 5. Evaluation of trading results
Evaluate your results using custom methods in your ```CustomAgent``` or using the collected results in the list ```backtest.results```.

