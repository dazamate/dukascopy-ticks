import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dukascopy_python
import pytz
import pandas as pd

class Dukascopy_Tick_Data_Fetcher:
    def __init__(self):
        self.broker_timezone = "Europe/Helsinki"

    def get(self, months_to_fetch: int, symbols: list[tuple[str, str]], output_dir: str, broker_timezone: str = None):
        raw_data_dir = os.path.join(output_dir, "raw_dukascopy_data")
        final_output_dir = os.path.join(output_dir, "processed_data")
        os.makedirs(raw_data_dir, exist_ok=True)
        os.makedirs(final_output_dir, exist_ok=True)

        target_tz_str = broker_timezone if broker_timezone is not None else self.broker_timezone
        target_tz = pytz.timezone(target_tz_str)
        print(f"Using broker timezone: {target_tz_str}")

        utc_tz = pytz.timezone("UTC")

        end_date = pd.Timestamp.now(tz=utc_tz)
        start_date = end_date - relativedelta(months=months_to_fetch)
        
        print(f"Required data range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')} UTC")

        for dukascopy_symbol, target_symbol in symbols:
            print("-" * 50)
            print(f"Processing symbol: {dukascopy_symbol} (Target: {target_symbol})")
            
            symbol_path = os.path.join(raw_data_dir, dukascopy_symbol.replace('/', '_'))
            os.makedirs(symbol_path, exist_ok=True)

            local_df = self._load_local_data(symbol_path, start_date, end_date)
            
            fetch_start_date = start_date
            dfs_to_combine = []

            if not local_df.empty:
                last_local_timestamp = local_df.index.max()
                print(f"Found local data up to {last_local_timestamp.strftime('%Y-%m-%d %H:%M')}")
                
                # Overlap rule: re-fetch from the start of the last day to ensure data completeness.
                fetch_start_date = last_local_timestamp.floor('D')
                
                # Discard the last partial day from local data to prevent duplicate entries after fetching.
                clean_local_df = local_df[local_df.index < fetch_start_date]
                if not clean_local_df.empty:
                    dfs_to_combine.append(clean_local_df)
            else:
                print("No local data found for this range.")

            if fetch_start_date < end_date:
                print(f"Fetching new data from Dukascopy: {fetch_start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                try:
                    new_df = self._fetch_from_dukascopy(dukascopy_symbol, fetch_start_date, end_date)
                    if not new_df.empty:
                        self._save_local_data(symbol_path, new_df)
                        dfs_to_combine.append(new_df)
                except Exception as e:
                    print(f"  ERROR fetching data for {dukascopy_symbol}: {e}")
                    continue
            else:
                print("Local data is up-to-date. No download needed.")

            if not dfs_to_combine:
                print(f"No data available for {dukascopy_symbol} in the total specified range.")
                continue

            raw_df = pd.concat(dfs_to_combine)
            raw_df = raw_df[~raw_df.index.duplicated(keep='first')]
            raw_df.sort_index(inplace=True)
            
            raw_df = raw_df.loc[start_date:end_date]

            if raw_df.empty:
                print(f"Final dataset is empty for {dukascopy_symbol} after processing.")
                continue

            processed_df = raw_df.tz_convert(target_tz)

            today_str = datetime.now().strftime('%Y-%m-%d')
            tz_suffix = target_tz_str.replace('/', '_')
            output_filename = f"{target_symbol}-{tz_suffix}-{today_str}.csv"
            output_path = os.path.join(final_output_dir, output_filename)
            
            processed_df.to_csv(output_path)
            print(f"Successfully saved processed data to: {output_path}")
            print(f"  First timestamp: {processed_df.index[0]}")
            print(f"  Last timestamp:  {processed_df.index[-1]}")

    def _load_local_data(self, symbol_path: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        dfs = []

        month_start = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        date_range = pd.date_range(month_start, end_date, freq='MS')
        
        for dt in date_range:
            file_path = os.path.join(symbol_path, str(dt.year), f"{dt.month:02d}.csv")
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, index_col='timestamp', date_format='ISO8601')
                    if not df.empty:
                        dfs.append(df)
                except Exception as e:
                    print(f"  Warning: Could not load or parse {file_path}. Error: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs).sort_index()
        return combined_df.loc[start_date:end_date]

    def _save_local_data(self, symbol_path: str, df_to_save: pd.DataFrame):
        for (year, month), group_df in df_to_save.groupby([df_to_save.index.year, df_to_save.index.month]):
            month_dir = os.path.join(symbol_path, str(year))
            os.makedirs(month_dir, exist_ok=True)
            file_path = os.path.join(month_dir, f"{month:02d}.csv")
            
            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path, index_col='timestamp', date_format='ISO8601')
                    
                combined_df = pd.concat([existing_df, group_df])
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df.sort_index(inplace=True)
                combined_df.to_csv(file_path)
            else:
                group_df.to_csv(file_path)
        print(f"  Saved/updated raw data in cache at: {symbol_path}")

    def _fetch_from_dukascopy(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        df = dukascopy_python.fetch(
            symbol,
            dukascopy_python.INTERVAL_TICK,
            dukascopy_python.OFFER_SIDE_BID,
            start_date,
            end_date,
        )
        return df

    def set_broker_timezone(self, timezone_name: str):
        try:
            pytz.timezone(timezone_name)
            self.broker_timezone = timezone_name
            print(f"Default broker timezone set to: {timezone_name}")
        except pytz.exceptions.UnknownTimeZoneError:
            print(f"Unknown timezone: {timezone_name}")