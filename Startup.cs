using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;
using youFast;

namespace clientMachine
{
    public class request
    {
        public string processID { get; set; }      
        public string processButton { get; set; }
        public string dataset{ get; set; }
        public string randomFilter { get; set; }
        public string userID { get; set; }
        public string drillType { get; set; }
        public decimal drillSet { get; set; }
        public string drillSetHeader { get; set; }
        public decimal drillSetCrosstab { get; set; }
        public string debugOutput { get; set; }
        public string importFile { get; set; }
        public string importType { get; set; }
        public string timeStamp { get; set; }
        public string filterColumn { get; set; }
        public string direction { get; set; }
        public string sortingOrder { get; set; }
        public string openReport { get; set; }
        public decimal nextPageID { get; set; }
        public decimal cancelRequestID { get; set; }
        public int pageXlength { get; set; }
        public int pageYlength { get; set; }
        public int pageXlengthCrosstab { get; set; }
        public int pageYlengthCrosstab { get; set; }
        public string rotateDimension { get; set; }
        public string rotateDimensionFrom { get; set; }
        public string rotateDimensionTo { get; set; }
        public string sortXdimension { get; set; }
        public string sortYdimension { get; set; }
        public string precisionLevel { get; set; }
        public string drillDownEventType { get; set; }
        public string moveColumnDirection { get; set; }
        public string moveColumnName { get; set; }
        public string addColumnType { get; set; }
        public string resetDimensionOrder { get; set; }
        public string measureType { get; set; }
        public List<string> column { get; set; }
        public List<string> startOption { get; set; }
        public List<string> startColumnValue { get; set; }
        public List<string> endOption { get; set; }
        public List<string> endColumnValue { get; set; }
        public List<string> distinctDimension { get; set; }
        public List<string> distinctOrder { get; set; }
        public List<string> crosstabDimension { get; set; }
        public List<string> crosstabDimensionTemp { get; set; }
        public List<string> crosstabOrder { get; set; }
        public List<string> measurement { get; set; }
    }

    public class response
    {
        public decimal sourcedRecordCount;
        public decimal selectedRecordCount = 0;
        public decimal distinctCount;
        public decimal crosstabCount;
        public string serverSession;
        public string closeServerSession;
        public decimal requestID;
        public bool periodExist;
        public int periodChangeColumn;
        public int periodEndColumn;
        public int periodRow;       
        public string dcDimensionName;       
        public bool sendErrorMessage = false;
        public bool calcBalance = false;
        public bool isBalanceTable = false;
        public StringBuilder errorMessage;     
        public bool uploadDesktopFile;
        public StringBuilder displaySelectedValueHtml;
        public Dictionary<string, int> memTable;       
        public bool removeCrosstabMeasure;
        public bool isMapping;
        public bool isMappingKeyFound;
        public bool extractFromCSV;
        public string currentMappingFile;
        public StringBuilder html;
        public StringBuilder htmlBackup;
        public DateTime startTime;
        public DateTime endTime;
    }
    public class userPreference
    {
        public string drillDownEventType;
        public int nextDrillDownEventType = 1;
        public int eventMonitorTimeout = 1000;
        public int eventMonitorSleep = 30;
        public bool saveTable2Memory = true; 
        public bool saveTable2Disk = false; 
        public int maxDiskTable = 0; //pending        
        public int maxDiskTableVersion = 0;  //pending
        public int maxImportFileVersion = 0;  //pending
        public double maxImportFileByte = 2000000000; 
        public int maxColumnThread = 16; 
        public int maxSegmentThread = 20; 
        public int maxActiveWorkspace = 1; //pending
        public int maxImportFileColumn = 2000000000; 
        public int maxImportFileRow = 2000000000; 
        public int maxStartupSampleTableSimulation = 100;
        public int maxExportFileRow = 2000000000;
        public int maxUniqueTextForEachDimension = 2000000000; 
        public int maxLengthOfInteger = 15; //pending
        public int maxLengthOfDecimalPlace = 6; //pending
        public int distinctRoundToDecimalPlace = 2; 
        public int maxCrosstabCell = 2000000000; 
        public int maxDistinctSet = 2000000000;
        public string os;
        public bool isWindowsServer;
        public string slash;
        public string websocketPort = "5001";
        public string httpPort = "8001";
        public string serverIP;
        public bool outputHTMLatStartup = false;
        public bool enableMoveColumn = true;  //pending
        public bool enableDrillRow = true; //pending           
        public List<string> assignMeasureAsDimension;       
    }
    public class clientSession
    {
        public List<string> clientSessionMeasurementName;
        public List<string> clientSessionCrosstabName;
        public List<string> clientSessionDistinctName;       
    }
    class clientMachine
    {
        static void Main(string[] args)
        {
            Console.Clear();

            ConcurrentDictionary<string, userPreference> userPreference = new ConcurrentDictionary<string, userPreference>();
            userPreference.TryAdd("system", new userPreference());

            OperatingSystem os = Environment.OSVersion;
            PlatformID pid = os.Platform;                       

            switch (pid)
            {
                case PlatformID.Win32NT:               
                case PlatformID.Win32S:
                case PlatformID.Win32Windows:
                case PlatformID.WinCE:
                {
                    string subKey = @"SOFTWARE\Wow6432Node\Microsoft\Windows NT\CurrentVersion";
                    Microsoft.Win32.RegistryKey key = Microsoft.Win32.Registry.LocalMachine;
                    Microsoft.Win32.RegistryKey skey = key.OpenSubKey(subKey);

                    if (skey.GetValue("ProductName").ToString().ToUpper().Contains("SERVER"))
                        userPreference["system"].os = "Windows Server";
                    else
                        userPreference["system"].os = "Windows Client";
                       
                        userPreference["system"].slash = "\\";
                         Console.WriteLine("youFast in-memory database server is running on " + skey.GetValue("ProductName").ToString() + " (" + os.Version.Major + "." + os.Version.Minor + "." + os.Version.Build + ")");                       
                    break;
                }                   
                case PlatformID.Unix:
                {                      
                    userPreference["system"].os = "Linux";
                    userPreference["system"].slash = "/";
                    Console.WriteLine("youFast in-memory database server is running on Linux");                   
                    break;
                }
                case PlatformID.MacOSX:
                {
                    userPreference["system"].os = "Mac";                     
                    break;
                }
                default:
                    Console.WriteLine("No Idea what I'm on!");
                    break;
            }

            int iteration = 1;
            string outputFolder = "uSpace" + userPreference["system"].slash + "exportedFile" + userPreference["system"].slash;
            string sourceFolder = "uSpace" + userPreference["system"].slash;
            string errorMessageLog = "uSpace" + userPreference["system"].slash + "exportedFile" + userPreference["system"].slash + "errorMessage" + userPreference["system"].slash;
            string db1Folder = "uSpace" + userPreference["system"].slash + "distinctDB" + userPreference["system"].slash + "Primary" + userPreference["system"].slash;
            string dbBackupFolder = "uSpace" + userPreference["system"].slash + "distinctDB" + userPreference["system"].slash + "Backup" + userPreference["system"].slash;
            byte csvReadSeparator = 44;
            
            char csvWriteSeparator = ',';

            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            if (!Directory.Exists(sourceFolder))
                Directory.CreateDirectory(sourceFolder);

            if (!Directory.Exists(db1Folder))
                Directory.CreateDirectory(db1Folder);

            if (!Directory.Exists(dbBackupFolder))
                Directory.CreateDirectory(dbBackupFolder);


            Dictionary<int, string> forwardMessage = new Dictionary<int, string>();
            ConcurrentDictionary<decimal, request> requestDict = new ConcurrentDictionary<decimal, request>();
            ConcurrentDictionary<decimal, response> responseDict = new ConcurrentDictionary<decimal, response>();           
            ConcurrentDictionary<string, clientSession> clientSessionVariable = new ConcurrentDictionary<string, clientSession>();
            userPreference.TryAdd("system", new userPreference());

            forwardMessage[0] = "waiting";
            WebSockAgentServer agentServer = new WebSockAgentServer();
            agentServer.webSock(errorMessageLog, userPreference, iteration, outputFolder, csvReadSeparator, csvWriteSeparator, forwardMessage, requestDict, responseDict, clientSessionVariable, sourceFolder, db1Folder, dbBackupFolder);                                 

            if (File.Exists("youFastSource" + userPreference["system"].slash + "data.cs"))
            {
                StringBuilder dataCS = new StringBuilder();

                string dataFolder = "youFastSource" + userPreference["system"].slash + "Data";
                string targetZipFile = "uSpace" + userPreference["system"].slash + "factoryData.zip";
                string targetCSFile = "youFastSource" + userPreference["system"].slash + "data.cs";

                if (File.Exists(targetZipFile))
                    File.Delete(targetZipFile);

                ZipFile.CreateFromDirectory(dataFolder, targetZipFile);

                byte[] readFile = File.ReadAllBytes(targetZipFile);

                dataCS.AppendLine("using System;");
                dataCS.AppendLine("using System.IO;");
                dataCS.AppendLine("using System.IO.Compression;");
                dataCS.AppendLine("namespace youFast");
                dataCS.AppendLine("{");
                dataCS.AppendLine("public class Data");
                dataCS.AppendLine("{");
                dataCS.AppendLine("public void data(string outputZipFile, string outputCSVData)");
                dataCS.AppendLine("{");
                dataCS.AppendLine("if (File.Exists(outputCSVData + \"factoryData.csv\"))");
                dataCS.AppendLine("File.Delete(outputCSVData + \"factoryData.csv\");");

                dataCS.Append("byte[] factoryData = {");

                int i = 0;
                foreach (byte value in readFile)
                {
                    dataCS.Append(value.ToString());
                    i++;

                    if (i < readFile.Length)
                        dataCS.Append(",");
                }

                dataCS.AppendLine("};");

                dataCS.AppendLine("File.WriteAllBytes(outputZipFile, factoryData);");
                dataCS.AppendLine("ZipFile.ExtractToDirectory(outputZipFile, outputCSVData);");

                dataCS.AppendLine("File.Delete(outputZipFile);");
                dataCS.AppendLine("}");
                dataCS.AppendLine("}");
                dataCS.AppendLine("}");

                if (File.Exists(targetCSFile))
                    File.Delete(targetCSFile);

                using (StreamWriter toDisk = new StreamWriter(targetCSFile))
                {
                    toDisk.Write(dataCS);
                    toDisk.Close();
                }
            }

            string outputZipFile = "uSpace" + userPreference["system"].slash + "factoryData.zip";
            string outputCSVData = "uSpace" + userPreference["system"].slash;

            Data currentData = new Data();
            currentData.data(outputZipFile, outputCSVData);
          
        }
    }
}


