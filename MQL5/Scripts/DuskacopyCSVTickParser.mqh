//+------------------------------------------------------------------+
//|                                          DuskacopyTickParser.mqh |
//|                                      Copyright 2025, Dale Woods. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2025, Dale Woods"
#property link      "https://www.mql5.com"
#property version   "1.00"

interface ITickParser {
   uint Parse(MqlTick &ticks_array[]) const;
};


class DuskacopyCSVTickParser : public ITickParser {
   private:
      string _filePath;
      bool _useCommonFolder;
      
      bool ParseDukascopyDateTime(string datetime_str, datetime &tick_time, long &tick_time_msc) const;
      
   public:
      DuskacopyCSVTickParser();
      
      bool SetFile(const string filePath, const bool common);
      
      uint Parse(MqlTick &ticks_array[]) const override;
};

DuskacopyCSVTickParser::DuskacopyCSVTickParser(): _filePath(NULL) {

}


bool DuskacopyCSVTickParser::SetFile(const string filePath, const bool common) {
   if (!FileIsExist(filePath, (common) ?  FILE_COMMON : 0)) {
        printf("Error: File: %s not found", _filePath);
        return false;
   }
   
   _filePath = filePath;
   _useCommonFolder = common;
   return true;
}


uint DuskacopyCSVTickParser::Parse(MqlTick &ticks_array[]) const override {
   int flags = FILE_READ|FILE_ANSI;
   
   if (_useCommonFolder) {
      flags |= FILE_COMMON;
   }

   int file_handle = FileOpen(_filePath, flags);   
    
   if (file_handle == INVALID_HANDLE) {
      printf("Error: Failed to open file: %s", _filePath);
      return -1;
   }
     
   // Variables for tick data
   const int reserve = 5000000;
   
   ArrayResize(ticks_array, 0, reserve);
   
   uint tick_count = 0;
   int imported_count = 0;
   int line_number = 0;
     
   // Skip header line if present
   if (!FileIsEnding(file_handle)) {
      string header = FileReadString(file_handle);
      //Print("Header: ", header);
      line_number++;
   }
    
    // Read tick data line by line
    while (!FileIsEnding(file_handle)) {
         line_number++;
        
        // Read entire line as string
        string csv_line = FileReadString(file_handle);
                
        // Skip empty lines
        if(StringLen(csv_line) == 0) continue;
        
        // Split by comma delimiter using proper MQL5 approach
        string csv_parts[];
        int csv_count = StringSplit(csv_line, StringGetCharacter(",", 0), csv_parts);
        
        // Check if we have enough columns
        if (csv_count < 5) {
            Print("Warning: Line ", line_number, " has only ", csv_count, " columns, expected 5");
            continue;
        }
        
        // Parse CSV columns based on actual format:
        // timestamp,bidPrice,askPrice,bidVolume,askVolume
        string datetime_str = csv_parts[0];
        double bid = StringToDouble(csv_parts[1]);
        double ask = StringToDouble(csv_parts[2]);
        double bid_volume = StringToDouble(csv_parts[3]);
        double ask_volume = StringToDouble(csv_parts[4]);
        
        // Parse date and time
        datetime tick_time;
        long tick_time_msc;
        
        if (!ParseDukascopyDateTime(datetime_str, tick_time, tick_time_msc)) {
            Print("Warning: Invalid datetime at line ", line_number, ": ", datetime_str);
            continue;
        }
        
        // Resize tick array
        ArrayResize(ticks_array, tick_count + 1, reserve);
        
        // Fill tick data
        ticks_array[tick_count].time = tick_time;
        ticks_array[tick_count].bid = bid;
        ticks_array[tick_count].ask = ask;
        ticks_array[tick_count].last = (bid + ask) / 2.0; // Mid price as last
        ticks_array[tick_count].volume = (long)(ask_volume + bid_volume);
        ticks_array[tick_count].time_msc = tick_time_msc; // Use parsed milliseconds
        ticks_array[tick_count].flags = TICK_FLAG_BID | TICK_FLAG_ASK;
        ticks_array[tick_count].volume_real = ask_volume + bid_volume;
        
        tick_count++;
        imported_count++;
        
        // Show progress
        if (imported_count % 50000 == 0) {
            Print("Processed ", imported_count, " ticks...");
        }
    }
    
    FileClose(file_handle);
    
    return tick_count;
}


bool DuskacopyCSVTickParser::ParseDukascopyDateTime(string datetime_str, datetime &tick_time, long &tick_time_msc) const {
    // Format: 2025-04-22 16:55:11.797000+02:00
    // Target: milliseconds since epoch (Jan 1, 1970)
    
    // Split by space to separate date and time parts
    string main_parts[];
    int main_count = StringSplit(datetime_str, ' ', main_parts);
    
    if(main_count < 2) {
        tick_time = 0;
        tick_time_msc = 0;
        return false;
    }
    
    string date_part = main_parts[0];  // 2025-04-22
    string time_timezone = main_parts[1]; // 16:55:11.797000+02:00
    
    // Remove timezone info (everything after + or -)
    string time_part = time_timezone;
    int plus_pos = StringFind(time_part, "+");
    int minus_pos = StringFind(time_part, "-", 1); // Start from position 1 to avoid negative times
    
    if(plus_pos > 0)
        time_part = StringSubstr(time_part, 0, plus_pos);
    else if(minus_pos > 0)
        time_part = StringSubstr(time_part, 0, minus_pos);
    
    // Parse date part (YYYY-MM-DD)
    string date_parts[];
    int date_count = StringSplit(date_part, '-', date_parts);
    
    if(date_count != 3)
    {
        tick_time = 0;
        tick_time_msc = 0;
        return false;
    }
    
    MqlDateTime dt;
    ZeroMemory(dt);
    
    dt.year = (int)StringToInteger(date_parts[0]);
    dt.mon = (int)StringToInteger(date_parts[1]);
    dt.day = (int)StringToInteger(date_parts[2]);
    
    // Parse time part (HH:MM:SS.microseconds)
    string time_parts[];
    int time_count = StringSplit(time_part, ':', time_parts);
    
    if(time_count < 3)
    {
        tick_time = 0;
        tick_time_msc = 0;
        return false;
    }
    
    dt.hour = (int)StringToInteger(time_parts[0]);
    dt.min = (int)StringToInteger(time_parts[1]);
    
    // Parse seconds and microseconds
    string sec_microsec_parts[];
    int sec_count = StringSplit(time_parts[2], '.', sec_microsec_parts);
    
    dt.sec = (int)StringToInteger(sec_microsec_parts[0]);
    
    // Extract milliseconds from microseconds
    int milliseconds = 0;
    if(sec_count > 1)
    {
        string microsec_str = sec_microsec_parts[1];
        
        // Ensure we have at least 3 digits for milliseconds
        while(StringLen(microsec_str) < 6)
            microsec_str = microsec_str + "0";
        
        // Take first 3 digits for milliseconds (ignore remaining microseconds)
        if(StringLen(microsec_str) >= 3)
        {
            string millisec_str = StringSubstr(microsec_str, 0, 3);
            milliseconds = (int)StringToInteger(millisec_str);
        }
    }
    
    // Convert to datetime (seconds since epoch)
    tick_time = StructToTime(dt);
    
    if(tick_time == 0)
    {
        tick_time_msc = 0;
        return false;
    }
    
    // Convert to milliseconds since epoch
    // According to MQL5 docs: time_msc takes priority if != 0
    tick_time_msc = (long)tick_time * 1000 + milliseconds;
    
    return true;
}
