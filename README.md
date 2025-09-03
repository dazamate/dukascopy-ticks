#Dukascopy Tick Fetcher for Metatrader 5 Use#

This module will:
    - fetch n months of tick data from the Dukascopy servers
    - cache the data downloaded
    - convert the tick timestamps into your broker's time zone and save as one big .csv

You can then use the .csv to import the ticks into a custom symbol.

Or you can use my MQL5 script
    - Copy all the compiled tick files to a folder in the MQL5\Files folder
    - run the script, it will use the filenames to detect the symbol and create a custom symbol like EURUSD.DUSKA with the tick data
    
Enjoy the free tick data for quality backtesting

```python
from dukascopy_tick_data_fetcher import Dukascopy_Tick_Data_Fetcher

# Initialize the fetcher for GMT+3
fetcher = Dukascopy_Tick_Data_Fetcher()

# set your broker's timezone so it matches with your broker's candle times and factors in day light savings
fetcher.set_broker_timezone("Europe/Helsinki")

# Define the symbols to fetch by making tuples of the duskacopy symbol and mapping it to your broker's verion of that symbol
symbols_to_get = [    
    ("XAU/USD", "XAUUSD"),
    ("GBP/AUD", "GBPAUD"),
    ("EUR/NZD", "EURNZD"),
    ("EUR/USD", "EURUSD"),
    ("GBP/JPY", "GBPJPY"),
    ("USD/JPY", "USDJPY"),
    ("USA500.IDX/USD", "US500"),
    ("USA30.IDX/USD", "US30")
]

# Define the output directories
output_directory = "tick_data"
broker_ticks_output_directory = "mql5_tick_data" # the directory where tick data compiled into a csv + tz conversion go

# Get the last 4 months of data
fetcher.get(
    months_to_fetch=4,
    symbols=symbols_to_get,
    tick_data_repo_dir=output_directory,
    broker_ticks_output_dir=broker_ticks_output_directory,
    date_suffix_on_output_csv_file=False
)
```

##unit testing##

##Setup Unit tests##
```bash
uv pip install -e ".[test]"
```

##Run unit tests##

```bash
pytest -v
```