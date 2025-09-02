//+------------------------------------------------------------------+
//|                                        DukascopyTickImporter.mq5 |
//|                                       Copyright 2025, Dale Woods |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2025, Your Company"
#property link      "https://www.mql5.com"
#property version   "1.00"
#property script_show_inputs

#include "DuskacopyCSVTickParser.mqh"

//--- Input parameters
input string DirPath = "DuskacopyTicks";              // CSV file name in MQL5/Files folder
input string CustomSymbolSuffix = ".DUSKA";           // Custom symbol prefix for storing data
input bool Use_Common_Folder = false;

//+------------------------------------------------------------------+
//| Script program start function                                    |
//+------------------------------------------------------------------+
void OnStart()
{
   string files[];
   
   GetFilesList(DirPath, Use_Common_Folder, files);
   
   for(int i=0; i<ArraySize(files); i++) {
      const string targetSymbol = GetSymbolFromFileName(files[i]);
      
      if (targetSymbol == NULL) continue;
      
      const string customName = StringFormat("%s.DUSKA", targetSymbol);
      const string filePath = StringFormat("%s\\%s", DirPath, files[i]);
      
      DuskacopyCSVTickParser parser;
      
      if (!parser.SetFile(filePath, Use_Common_Folder)) return;
      
      MqlTick ticks_array[];
      
      uint ticks = parser.Parse(ticks_array);
            
      MT5CustomChart chart(customName);
      
      if (chart.Exists()) {
         chart.Delete();
         Sleep(1000);
      }
       
      chart.CreateChart(targetSymbol);
      
      chart.Replace(ticks_array);
      Sleep(1000);
      
      chart.Select(); 
   }  
}


//+------------------------------------------------------------------+
//| Get list of files in directory                                   |
//+------------------------------------------------------------------+
void GetFilesList(string directory_path, const bool useCommonFolder, string &fileNames[], string file_mask = "*.csv") {
    string filename;
    long search_handle;
    
    ArrayResize(fileNames, 0, 10);
    
    // Start the search
    search_handle = FileFindFirst(directory_path + "\\" + file_mask, filename, (useCommonFolder) ? FILE_COMMON : 0);
    
    if (search_handle != INVALID_HANDLE) {
        Array::Push(filename, fileNames, 10);
                
        // Continue searching for more files
        while(FileFindNext(search_handle, filename)) {
            Array::Push(filename, fileNames, 10);
        }
        
        // Close the search handle
        FileFindClose(search_handle);
    }
    
    else {
        Print("No files found or directory doesn't exist: " + directory_path);
    }
}

string GetSymbolFromFileName(const string fileName) {
   string parts[];
   
   StringSplit(fileName, '-', parts);
   
   if (ArraySize(parts) < 2) {
      printf("Filename in unexpected format, should be prefixed with symbol-*.csv");
      return NULL;
   }
   
   return parts[0];
}
