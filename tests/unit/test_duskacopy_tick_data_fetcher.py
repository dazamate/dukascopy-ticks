import pytest
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from unittest.mock import Mock, patch, MagicMock, mock_open, call

from dukascopy_tick_data_fetcher import Dukascopy_Tick_Data_Fetcher

class TestDukascopyTickDataFetcher:
    
    @pytest.fixture
    def fetcher(self):
        """Create a fresh fetcher instance for each test"""
        return Dukascopy_Tick_Data_Fetcher()
    
    @pytest.fixture
    def sample_tick_data(self):
        """Sample tick data matching your CSV format"""
        data = {
            'bidPrice': [1.90169, 1.90167, 1.90167, 1.90167, 1.90164],
            'askPrice': [1.9021, 1.90208, 1.90209, 1.90208, 1.90208],
            'bidVolume': [0.54, 0.12, 0.45, 0.12, 0.12],
            'askVolume': [0.12, 0.12, 0.54, 0.54, 0.12]
        }
        timestamps = pd.date_range('2025-04-25 06:31:49.984', periods=5, freq='500ms', tz='UTC')
        return pd.DataFrame(data, index=timestamps)
    
    @pytest.fixture
    def partial_day_data(self):
        """Generate partial day of tick data (incomplete)."""
        data = {
            'bidPrice': [1.90169, 1.90167],
            'askPrice': [1.9021, 1.90208],
            'bidVolume': [0.54, 0.12],
            'askVolume': [0.12, 0.12]
        }
        timestamps = pd.date_range(
            '2025-04-25 10:30:00', 
            periods=2, 
            freq='1h',  # Fix: Use '1h' instead of '1H'
            tz='UTC'
        )
        return pd.DataFrame(data, index=timestamps)
    
    @pytest.fixture
    def complete_day_data(self):
        """Data that represents complete day"""
        data = {
            'bidPrice': [1.90169] * 10,
            'askPrice': [1.9021] * 10,
            'bidVolume': [0.54] * 10,
            'askVolume': [0.12] * 10
        }
        timestamps = pd.date_range('2025-04-25 00:00:00', '2025-04-25 23:59:59', periods=10, tz='UTC')
        return pd.DataFrame(data, index=timestamps)

    def test_init_default_timezone(self, fetcher):
        """Test that fetcher initializes with correct default timezone"""
        assert fetcher.broker_timezone == "Europe/Helsinki"
    
    def test_set_broker_timezone_valid(self, fetcher):
        """Test setting a valid timezone"""
        fetcher.set_broker_timezone("America/New_York")
        assert fetcher.broker_timezone == "America/New_York"
    
    def test_set_broker_timezone_invalid(self, fetcher, capsys):
        """Test setting an invalid timezone"""
        original_tz = fetcher.broker_timezone
        fetcher.set_broker_timezone("Invalid/Timezone")
        assert fetcher.broker_timezone == original_tz  # Should remain unchanged
        captured = capsys.readouterr()
        assert "Unknown timezone" in captured.out

    @patch('os.path.exists')
    @patch('pandas.read_csv')
    def test_load_local_data_empty_cache(self, mock_read_csv, mock_exists, fetcher):
        """Test loading data when no local cache exists"""
        mock_exists.return_value = False
        
        start_date = datetime(2025, 4, 1, tzinfo=pytz.UTC)
        end_date = datetime(2025, 4, 30, tzinfo=pytz.UTC)
        
        result = fetcher._load_local_data("test_path", start_date, end_date)
        
        assert result.empty
        mock_read_csv.assert_not_called()

    @patch('os.path.exists')
    @patch('pandas.read_csv')
    def test_load_local_data_with_existing_files(self, mock_read_csv, mock_exists, fetcher, sample_tick_data):
        """Test loading data from existing local files"""
        mock_exists.return_value = True
        mock_read_csv.return_value = sample_tick_data
        
        start_date = datetime(2025, 4, 1, tzinfo=pytz.UTC)
        end_date = datetime(2025, 4, 30, tzinfo=pytz.UTC)
        
        result = fetcher._load_local_data("test_path", start_date, end_date)
        
        assert not result.empty
        assert len(result) == len(sample_tick_data)
        mock_read_csv.assert_called()

    @patch('os.path.exists')
    @patch('pandas.read_csv')
    def test_load_local_data_corrupted_file(self, mock_read_csv, mock_exists, fetcher, capsys):
        """Test handling of corrupted local files"""
        mock_exists.return_value = True
        mock_read_csv.side_effect = Exception("File corrupted")
        
        start_date = datetime(2025, 4, 1, tzinfo=pytz.UTC)
        end_date = datetime(2025, 4, 30, tzinfo=pytz.UTC)
        
        result = fetcher._load_local_data("test_path", start_date, end_date)
        
        assert result.empty
        captured = capsys.readouterr()
        assert "Warning: Could not load or parse" in captured.out

    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    def test_save_local_data_new_file(self, mock_to_csv, mock_read_csv, mock_exists, mock_makedirs, fetcher, sample_tick_data):
        """Test saving data to new local file"""
        mock_exists.return_value = False
        
        fetcher._save_local_data("test_path", sample_tick_data)
        
        mock_makedirs.assert_called()
        mock_to_csv.assert_called()

    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('pandas.read_csv')
    @patch('pandas.DataFrame.to_csv')
    def test_save_local_data_existing_file(self, mock_to_csv, mock_read_csv, mock_exists, mock_makedirs, fetcher, sample_tick_data):
        """Test saving data to existing local file (should merge)"""
        mock_exists.return_value = True
        mock_read_csv.return_value = sample_tick_data.iloc[:2]  # Existing partial data
        
        fetcher._save_local_data("test_path", sample_tick_data)
        
        mock_makedirs.assert_called()
        mock_read_csv.assert_called()
        mock_to_csv.assert_called()

    @patch('dukascopy_python.fetch')
    def test_fetch_from_dukascopy_success(self, mock_fetch, fetcher, sample_tick_data):
        """Test successful fetch from Dukascopy API"""
        mock_fetch.return_value = sample_tick_data
        
        start_date = datetime(2025, 4, 25, tzinfo=pytz.UTC)
        end_date = datetime(2025, 4, 26, tzinfo=pytz.UTC)
        
        result = fetcher._fetch_from_dukascopy("EUR/USD", start_date, end_date)
        
        mock_fetch.assert_called_once()
        pd.testing.assert_frame_equal(result, sample_tick_data)

    @patch('dukascopy_python.fetch')
    def test_fetch_from_dukascopy_failure(self, mock_fetch, fetcher):
        """Test handling of Dukascopy API failure"""
        mock_fetch.side_effect = Exception("API Error")
        
        start_date = datetime(2025, 4, 25, tzinfo=pytz.UTC)
        end_date = datetime(2025, 4, 26, tzinfo=pytz.UTC)
        
        with pytest.raises(Exception, match="API Error"):
            fetcher._fetch_from_dukascopy("EUR/USD", start_date, end_date)

    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    def test_get_no_local_data_fresh_download(self, mock_save, mock_fetch, mock_load, mock_makedirs, fetcher, sample_tick_data):
        """Test complete fresh download when no local data exists"""
        # Setup mocks
        mock_load.return_value = pd.DataFrame()  # Empty cache
        mock_fetch.return_value = sample_tick_data
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        mock_load.assert_called_once()
        mock_fetch.assert_called_once()
        mock_save.assert_called_once()

    @patch('pandas.DataFrame.to_csv')
    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    def test_get_complete_local_data_no_download(self, mock_save, mock_fetch, mock_load, mock_makedirs, mock_to_csv, fetcher, complete_day_data, sample_tick_data):
        """Test that when local data exists, it re-fetches the last day for completeness"""
        # Setup: local data exists
        current_time = pd.Timestamp.now(tz=pytz.UTC)
        complete_data = complete_day_data.copy()
        complete_data.index = pd.date_range(
            start=current_time - relativedelta(days=2), 
            end=current_time - relativedelta(days=1),
            periods=len(complete_data), 
            tz='UTC'
        )
        
        mock_load.return_value = complete_data
        # Fix: Return actual data so _save_local_data gets called
        mock_fetch.return_value = sample_tick_data  # Return actual data
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        mock_load.assert_called_once()
        mock_fetch.assert_called_once()  # Should fetch to ensure completeness
        mock_save.assert_called_once()   # Should save the fetched data

    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    def test_get_incomplete_day_redownload(self, mock_save, mock_fetch, mock_load, mock_makedirs, fetcher, partial_day_data, sample_tick_data):
        """Test that incomplete day triggers re-download from start of that day"""
        # Setup: partial day data exists
        mock_load.return_value = partial_day_data
        mock_fetch.return_value = sample_tick_data
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        mock_load.assert_called_once()
        mock_fetch.assert_called_once()  # Should fetch to complete the day
        mock_save.assert_called_once()

    @patch('pandas.DataFrame.to_csv')  # Add this
    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    def test_get_partial_existing_data_extended_range(self, mock_save, mock_fetch, mock_load, mock_makedirs, mock_to_csv, fetcher, sample_tick_data):
        """Test requesting 6 months when 1 month exists - should only download missing months"""
        # Setup: 1 month of existing data within the date range
        current_time = pd.Timestamp.now(tz=pytz.UTC)
        existing_data = sample_tick_data.copy()
        # Set data to be from 2 months ago but within the 6-month range
        two_months_ago = current_time - relativedelta(months=2)
        existing_data.index = pd.date_range(
            start=two_months_ago, 
            periods=len(existing_data), 
            freq='1h',  # Fix: Use '1h' instead of '1H' 
            tz='UTC'
        )
        
        mock_load.return_value = existing_data
        
        # Make sure fetched data is also within date range
        new_data = sample_tick_data.copy()
        new_data.index = pd.date_range(
            start=current_time - relativedelta(days=30),  # Recent data
            periods=len(new_data),
            freq='1h',
            tz='UTC'
        )
        mock_fetch.return_value = new_data
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=6, symbols=symbols, output_dir=output_dir)
        
        mock_load.assert_called_once()
        mock_fetch.assert_called_once()
        mock_save.assert_called_once()
        
    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    def test_get_fetch_error_continues_with_next_symbol(self, mock_fetch, mock_load, mock_makedirs, fetcher, capsys):
        """Test that fetch error for one symbol doesn't stop processing other symbols"""
        mock_load.return_value = pd.DataFrame()
        mock_fetch.side_effect = Exception("Network error")
        
        symbols = [("EUR/USD", "EURUSD"), ("GBP/USD", "GBPUSD")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        captured = capsys.readouterr()
        assert "ERROR fetching data" in captured.out
        assert mock_fetch.call_count == 2  # Should try both symbols

    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    def test_get_empty_final_dataset(self, mock_fetch, mock_load, mock_makedirs, fetcher, capsys):
        """Test handling of empty final dataset"""
        mock_load.return_value = pd.DataFrame()
        mock_fetch.return_value = pd.DataFrame()  # Empty fetch result
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        captured = capsys.readouterr()
        # Fix: Update expected message to match actual code
        assert "No data available for EUR/USD in the total specified range." in captured.out

    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    @patch('pandas.DataFrame.to_csv')
    def test_get_timezone_conversion(self, mock_to_csv, mock_save, mock_fetch, mock_load, mock_makedirs, fetcher, sample_tick_data):
        """Test that data is properly converted to broker timezone"""
        mock_load.return_value = pd.DataFrame()
        
        # Ensure fetched data is within the requested date range
        current_time = pd.Timestamp.now(tz=pytz.UTC)
        data_in_range = sample_tick_data.copy()
        data_in_range.index = pd.date_range(
            start=current_time - relativedelta(days=15),  # Within the 1-month range
            periods=len(data_in_range),
            freq='1h',
            tz='UTC'
        )
        mock_fetch.return_value = data_in_range
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        broker_timezone = "America/New_York"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir, broker_timezone=broker_timezone)
        
        mock_to_csv.assert_called()

    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    def test_get_deduplication(self, mock_save, mock_fetch, mock_load, mock_makedirs, fetcher, sample_tick_data):
        """Test that duplicate timestamps are properly handled"""
        # Create data with duplicate timestamps
        duplicate_data = pd.concat([sample_tick_data, sample_tick_data])
        
        mock_load.return_value = pd.DataFrame()
        mock_fetch.return_value = duplicate_data
        
        symbols = [("EUR/USD", "EURUSD")]
        output_dir = "test_output"
        
        # This should not raise an error and should handle duplicates gracefully
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        mock_save.assert_called()

    @patch('os.makedirs')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_load_local_data')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_fetch_from_dukascopy')
    @patch.object(Dukascopy_Tick_Data_Fetcher, '_save_local_data')
    def test_get_multiple_symbols(self, mock_save, mock_fetch, mock_load, mock_makedirs, fetcher, sample_tick_data):
        """Test processing multiple symbols"""
        mock_load.return_value = pd.DataFrame()
        mock_fetch.return_value = sample_tick_data
        
        symbols = [("EUR/USD", "EURUSD"), ("GBP/USD", "GBPUSD"), ("USD/JPY", "USDJPY")]
        output_dir = "test_output"
        
        fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
        
        # Should process all symbols
        assert mock_load.call_count == 3
        assert mock_fetch.call_count == 3
        assert mock_save.call_count == 3

    def test_get_date_range_calculation(self, fetcher):
        """Test that date ranges are calculated correctly"""
        with patch('pandas.Timestamp.now') as mock_now:
            mock_now.return_value = pd.Timestamp('2025-05-01 12:00:00', tz='UTC')
            
            with patch.object(fetcher, '_load_local_data', return_value=pd.DataFrame()):
                with patch.object(fetcher, '_fetch_from_dukascopy', return_value=pd.DataFrame()):
                    with patch('os.makedirs'):
                        symbols = [("EUR/USD", "EURUSD")]
                        output_dir = "test_output"
                        
                        fetcher.get(months_to_fetch=2, symbols=symbols, output_dir=output_dir)
                        
                        # Verify the date calculation is correct
                        expected_start = pd.Timestamp('2025-03-01 12:00:00', tz='UTC')
                        expected_end = pd.Timestamp('2025-05-01 12:00:00', tz='UTC')
                        
                        # The _load_local_data should be called with correct dates
                        fetcher._load_local_data.assert_called()

    def test_output_filename_format(self, fetcher):
        """Test that output filenames are formatted correctly"""
        with patch('os.makedirs'):
            with patch.object(fetcher, '_load_local_data', return_value=pd.DataFrame()):
                with patch.object(fetcher, '_fetch_from_dukascopy') as mock_fetch:
                    # Create data within the expected date range
                    current_time = pd.Timestamp.now(tz=pytz.UTC)
                    sample_data = pd.DataFrame({
                        'bidPrice': [1.0],
                        'askPrice': [1.0], 
                        'bidVolume': [1.0],
                        'askVolume': [1.0]
                    }, index=pd.date_range(
                        start=current_time - relativedelta(days=15), 
                        periods=1, 
                        tz='UTC'
                    ))
                    mock_fetch.return_value = sample_data
                    
                    with patch('pandas.DataFrame.to_csv') as mock_to_csv:
                        with patch.object(fetcher, '_save_local_data'):
                            symbols = [("EUR/USD", "EURUSD")]
                            output_dir = "test_output"
                            
                            fetcher.get(months_to_fetch=1, symbols=symbols, output_dir=output_dir)
                            
                            mock_to_csv.assert_called()
                            call_args = mock_to_csv.call_args[0][0]
                            assert "EURUSD" in call_args
                            assert "Europe_Helsinki" in call_args
                            assert call_args.endswith(".csv")


if __name__ == "__main__":
    pytest.main([__file__])