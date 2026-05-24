import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dukascopy_python
import pytz
import pandas as pd

class Dukascopy_Tick_Data_Fetcher:
    def __init__(self):
        self.broker_timezone = "Europe/Helsinki"

    def get(
            self,
            months_to_fetch: int,
            symbols: list[tuple[str, str]],
            tick_data_repo_dir: str,
            broker_ticks_output_dir: str,
            broker_timezone: str = None,
            date_suffix_on_output_csv_file: bool = False
        ):
        os.makedirs(tick_data_repo_dir, exist_ok=True)
        os.makedirs(broker_ticks_output_dir, exist_ok=True)

        target_tz_str = broker_timezone if broker_timezone is not None else self.broker_timezone
        target_tz = pytz.timezone(target_tz_str)
        print(f"Using broker timezone: {target_tz_str}")

        utc_tz = pytz.timezone("UTC")

        end_date = pd.Timestamp.now(tz=utc_tz)
        start_date = end_date - relativedelta(months=months_to_fetch)
        
        print(f"Required data range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')} UTC")

        tz_suffix = target_tz_str.replace('/', '_')

        for dukascopy_symbol, target_symbol in symbols:
            print("-" * 50)
            print(f"Processing symbol: {dukascopy_symbol} (Target: {target_symbol})")

            symbol_path = os.path.join(tick_data_repo_dir, dukascopy_symbol.replace('/', '_'))
            os.makedirs(symbol_path, exist_ok=True)

            if date_suffix_on_output_csv_file:
                today_str = datetime.now().strftime('%Y-%m-%d')
                output_filename = f"{target_symbol}-{tz_suffix}-{today_str}.csv"
            else:
                output_filename = f"{target_symbol}-{tz_suffix}.csv"
            output_path = os.path.join(broker_ticks_output_dir, output_filename)

            first_month_output = True
            month_cursor = start_date
            total_ticks = 0

            while month_cursor < end_date:
                month_start = month_cursor.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                month_end = month_start + relativedelta(months=1)
                if month_end > end_date:
                    month_end = end_date

                print(f"  Month {month_start.strftime('%Y-%m')}: ", end="", flush=True)

                local_df = self._load_local_data(symbol_path, month_start, month_end)

                fetch_start = month_start
                dfs_to_combine = []

                if not local_df.empty:
                    last_local_ts = local_df.index.max()
                    fetch_start = last_local_ts.floor('D')
                    clean_local = local_df[local_df.index < fetch_start]
                    if not clean_local.empty:
                        dfs_to_combine.append(clean_local)

                if fetch_start < month_end:
                    try:
                        new_df = self._fetch_from_dukascopy(dukascopy_symbol, fetch_start, month_end)
                        if not new_df.empty:
                            self._save_local_data(symbol_path, new_df)
                            dfs_to_combine.append(new_df)
                    except Exception as e:
                        print(f"ERROR: {e}")
                else:
                    print("cached", end="", flush=True)

                if not dfs_to_combine:
                    print(" - no data")
                    month_cursor = month_end
                    continue

                month_df = pd.concat(dfs_to_combine)
                month_df = month_df[~month_df.index.duplicated(keep='first')]
                month_df.sort_index(inplace=True)

                clip_start = max(month_start, start_date)
                clip_end = min(month_end, end_date)
                month_df = month_df.loc[clip_start:clip_end]

                if month_df.empty:
                    print(" - empty after clip")
                    month_cursor = month_end
                    continue

                month_df = month_df.tz_convert(target_tz)

                if first_month_output:
                    month_df.to_csv(output_path, mode='w')
                    first_month_output = False
                else:
                    month_df.to_csv(output_path, mode='a', header=False)

                month_ticks = len(month_df)
                total_ticks += month_ticks
                print(f"{month_ticks} ticks")

                month_cursor = month_end

            if total_ticks == 0:
                print(f"  No data available for {dukascopy_symbol} in the specified range.")
                continue

            print(f"  Saved {total_ticks} ticks to {output_path}")

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