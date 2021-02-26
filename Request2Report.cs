using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using Fleck;

namespace youFast
{
    public class Request2Report
    {
        public void distinctDBreporting(ConcurrentDictionary<string, bool> isResponse, ConcurrentDictionary<decimal, IWebSocketConnection> requestID2SessionID, List<IWebSocketConnection> wsServerSessionID, string errorMessageLog, Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping, ConcurrentDictionary<string, int> copyMemDetailRandomly, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<int, decimal> currentRequestID, char csvWriteSeparator, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, StringBuilder> htmlTable, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, string sourceFolder, string outputFolder, byte csvReadSeparator, string db1Folder, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl)
        {
            requestDict[requestID].importFile = requestDict[requestID].importFile.Replace("?", " ");           

            if (!requestDict[requestID].importFile.Contains("BalanceTable") && requestDict[requestID].crosstabDimension != null)
            {
                if (requestDict[requestID].crosstabDimension.Contains("Period#End"))
                {
                    List<string> tempCrosstab = new List<string>();
                    List<string> periodEnd = new List<string>();
                    List<string> tempDistinct = new List<string>();

                    tempCrosstab.Add("Period#End");
                    for (int i = 0; i < requestDict[requestID].crosstabDimension.Count; i++)
                    {                        
                        if(requestDict[requestID].crosstabDimension[i].ToString() != "Period#End" && requestDict[requestID].crosstabDimension[i].ToString() != "Period#Change")
                            tempCrosstab.Add(requestDict[requestID].crosstabDimension[i].ToString());
                    }

                    for (int i = 0; i < requestDict[requestID].distinctDimension.Count; i++)
                        tempDistinct.Add(requestDict[requestID].distinctDimension[i].ToString());

                    
                    for (int i = 0; i < tempCrosstab.Count; i++)
                        if (!tempDistinct.Contains(tempCrosstab[i]))
                            tempDistinct.Add(tempCrosstab[i]);                            

                    periodEnd.Add("Period#End");

                    requestDict[requestID].crosstabDimensionTemp = tempCrosstab;
                    requestDict[requestID].distinctDimension = tempDistinct;
                    requestDict[requestID].crosstabDimension = periodEnd;
                }
            }


            StringBuilder htmlEmpty = new StringBuilder();
            responseDict[requestID].sendErrorMessage = false;
            htmlEmpty.Append("");
            bool exit = false;

            if (!responseDict.ContainsKey(requestID))
                responseDict.TryAdd(requestID, new clientMachine.response());            

            responseDict[requestID].html = htmlEmpty;
            responseDict[requestID].requestID = requestID;

            Dictionary<int, List<double>> ramDetailgz = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> ramKey2Valuegz = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> ramValue2Keygz = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, Dictionary<double, double>> ramKey2Order = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> ramOrder2Key = new Dictionary<int, Dictionary<double, double>>();          

            bool useMemory = false;                     
            responseDict[requestID].uploadDesktopFile = false;
            string[] readFile = Directory.GetFiles(sourceFolder, requestDict[requestID].importFile.ToString()); // check if exist in csv import folder
            
            if (requestDict[requestID].importType == "overwrite"  && readFile.Length == 0) // use drag and drop file if not exist in csv import folder
            {                
                responseDict[requestID].uploadDesktopFile = true;
                string folder = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + userPreference["system"].slash;

                try
                {
                    File.Copy(folder + userPreference["system"].slash + requestDict[requestID].importFile.ToString(), sourceFolder + userPreference["system"].slash + requestDict[requestID].importFile.ToString(), true);
                }
                catch (IOException iox)
                {
                    Console.WriteLine(iox.Message);
                }

                readFile = Directory.GetFiles(sourceFolder, requestDict[requestID].importFile.ToString());
                StringBuilder builder = new StringBuilder();
                foreach (string value in readFile)
                    builder.Append(value);
            }

            Request2Report currentReport = new Request2Report();
            ConcurrentDictionary<decimal, Thread> exceptionThread = new ConcurrentDictionary<decimal, Thread>();
            ConcurrentQueue<decimal> exceptionQueue = new ConcurrentQueue<decimal>();            

            if (userPreference["system"].saveTable2Disk == false && userPreference["system"].saveTable2Memory == true && ramK2V.Count > 0 && ramK2V.ContainsKey(requestDict[requestID].importFile.ToString()))
                useMemory = true;
            else
            {
                if (useMemory == false && readFile.Length > 0) // use csv datastore if csv file list exist
                {                  
                    currentReport.importFile(isResponse, errorMessageLog, csvWriteSeparator, ramMapping, userPreference, copyMemDetailRandomly, outputFolder, responseDict, readFile, ramDetail, ramK2V, ramKey2Valuegz, sourceFolder, csvReadSeparator, db1Folder, requestID, clientSessionVariable, requestDict,  ramDetailgz);

                     ExportCSV currentExport = new ExportCSV();
                     currentExport.dimensionMasterData2CSV(userPreference, csvWriteSeparator, ramK2V, requestID, clientSessionVariable, requestDict,  outputFolder);

                    if (requestDict[requestID].importFile == "factoryData.csv")
                    {
                        Simulation currentSimulation = new Simulation();
                        currentSimulation.dimensionValueList(userPreference, copyMemDetailRandomly, ramDetail, ramK2V, clientSessionVariable, requestDict,  requestID, sourceFolder, outputFolder);
                    }
                }
            }

            try // new a thread to manage queue job
            {             

                exceptionThread.TryAdd(1, new Thread(() => exceptionalQueue()));
                exceptionThread[1].Start();
            }

            catch (Exception e)
            {
                Console.WriteLine($"exceptionThread fail '{e}'");
            }

            posting();

            void exceptionalQueue()
            {
                var lastRequestID = requestID;
                responseDict[requestID].sendErrorMessage = false;
                responseDict[requestID].calcBalance = false;
                responseDict[requestID].isBalanceTable = false;
                do
                {                   
                    if (responseDict[requestID].sendErrorMessage == true)
                    {                       
                        currentReport.importFile(isResponse, errorMessageLog, csvWriteSeparator, ramMapping, userPreference, copyMemDetailRandomly, outputFolder, responseDict, readFile, ramDetail, ramK2V, ramKey2Valuegz, sourceFolder, csvReadSeparator, db1Folder, requestID, clientSessionVariable, requestDict,  ramDetailgz);
                        requestDict[requestID].randomFilter = "Y";
                        responseDict[requestID].selectedRecordCount = 0;
                        posting();
                    }

                    if(responseDict[requestID].calcBalance == true)
                    {                        
                        requestDict[requestID].randomFilter = "N";
                        responseDict[requestID].selectedRecordCount = 0;                       
                        responseDict[requestID].calcBalance = false;
                        responseDict[requestID].isBalanceTable = true;

                        posting();
                    }

                    Thread.Sleep(userPreference["system"].eventMonitorSleep);

                    exit = true;
                    foreach (var client in wsServerSessionID)
                    {
                        if (responseDict.ContainsKey(requestID))
                            if (responseDict[requestID].serverSession == client.ConnectionInfo.Id.ToString())
                                exit = false;
                    }

                    var thisRequestID = requestDict.Keys.Max();                 

                    if (responseDict[lastRequestID].serverSession == responseDict[thisRequestID].serverSession)
                        if (requestDict[lastRequestID].importFile != requestDict[thisRequestID].importFile)
                            exit = true;

                    if (responseDict[lastRequestID].serverSession == responseDict[thisRequestID].serverSession)
                        if (responseDict[lastRequestID].requestID != responseDict[thisRequestID].requestID)
                            exit = true;

                } while (exit == false);
            }

            void posting()
            {   
                if (ramDetail.ContainsKey(requestDict[requestID].importFile))  // use ram datastore if filename exist in memory
                {
                    for (int i = 0; i < ramDetail[requestDict[requestID].importFile].Count; i++)
                        ramDetailgz[i] = ramDetail[requestDict[requestID].importFile][i];

                    ramKey2Valuegz.Clear();
                    for (int i = 0; i < ramK2V[requestDict[requestID].importFile].Count; i++)
                    {
                        ramKey2Valuegz.Add(i, new Dictionary<double, string>());
                        ramKey2Valuegz[i] = ramK2V[requestDict[requestID].importFile][i];
                    }
                    useMemory = true;
                }
                
                if (exit == true) // to restart exceptionThread for new imported data
                {
                    exceptionThread.TryAdd(2, new Thread(() => exceptionalQueue()));
                    exceptionThread[2].Start();
                }

                if (useMemory == false && readFile.Length == 0) // use disk DB datastoe if csv file not exist               
                    currentReport.importDB(userPreference, ramDetail, ramK2V, ramKey2Valuegz, sourceFolder, csvReadSeparator, db1Folder, requestID, clientSessionVariable, requestDict,  ramDetailgz);                

                if (tableFact.ContainsKey(requestDict[requestID].importFile)) // build table fact statistics           
                    tableFact[requestDict[requestID].importFile].Clear();

                tableFact.TryAdd(requestDict[requestID].importFile, new ConcurrentDictionary<int, int>());

                for (int i = 10; i < ramKey2Valuegz.Count + 10; i++)
                    tableFact[requestDict[requestID].importFile].TryAdd(i, ramKey2Valuegz[i - 10].Count); // master record count for each column

                int masterRecordCount = 0;
                for (int i = 0; i < ramKey2Valuegz.Count; i++)
                    masterRecordCount = masterRecordCount + tableFact[requestDict[requestID].importFile][i + 10];

                tableFact[requestDict[requestID].importFile].TryAdd(9, masterRecordCount); // total master record count for all columns
                tableFact[requestDict[requestID].importFile].TryAdd(8, ramDetailgz[0].Count - 1);  // total row                                     

                Dictionary<string, string> columnName2ID = new Dictionary<string, string>();

                for (int i = 0; i < ramK2V[requestDict[requestID].importFile].Count; i++)
                {
                    var columnName = ramK2V[requestDict[requestID].importFile][i][0];

                    if (!columnName2ID.ContainsKey(columnName))
                        columnName2ID.Add(ramK2V[requestDict[requestID].importFile][i][0], i.ToString());
                }

                if ((requestDict[requestID].randomFilter == "Y" && responseDict[requestID].isBalanceTable != true) || requestDict[requestID].startColumnValue == null) // random generate filter
                {
                    Dictionary<string, string> variable = new Dictionary<string, string>();
                    Dictionary<string, List<string>> array = new Dictionary<string, List<string>>();

                    List<string> displayRandomSelectedValue = new List<string>();
                    List<string> selectDistinctCol = new List<string>();

                    currentReport.randomGenerateSelectionCriteria(userPreference, variable, array, requestID, clientSessionVariable, requestDict,  responseDict, ramK2V, columnName2ID, selectDistinctCol, ramDetail, displayRandomSelectedValue);                    
                    ExportHTML currentExport = new ExportHTML();
                    int serialID = 0;                                   
                    currentExport.ramdistinct2DisplaySelectedFilterValue(isResponse, requestID2SessionID, wsServerSessionID, userPreference, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, serialID, ramK2V[requestDict[requestID].importFile], displayRandomSelectedValue);
                  
                    requestDict[requestID].processID = "refreshSelectFileList";                   
                }                
                else // validation of selection criteria
                {
                    List<string> tempStartColumnValue = new List<string>(); // validation of column value
                    List<string> tempEndColumnValue = new List<string>();

                    for (int i = 0; i < requestDict[requestID].startColumnValue.Count; i++)
                    {
                        tempStartColumnValue.Add(requestDict[requestID].startColumnValue[i]);
                        tempEndColumnValue.Add(requestDict[requestID].endColumnValue[i]);

                        if (requestDict[requestID].startColumnValue[i] == "blankValue" && requestDict[requestID].endColumnValue[i] != "blankValue")
                            requestDict[requestID].startColumnValue[i] = requestDict[requestID].endColumnValue[i];

                        if (requestDict[requestID].endColumnValue[i] == "blankValue" && requestDict[requestID].startColumnValue[i] != "blankValue")
                            requestDict[requestID].endColumnValue[i] = requestDict[requestID].startColumnValue[i];

                        if (string.Compare(requestDict[requestID].endColumnValue[i], requestDict[requestID].startColumnValue[i]) < 0)
                            requestDict[requestID].column[i] = "Fact";

                        if (requestDict[requestID].startColumnValue[i] == "blankValue" && requestDict[requestID].endColumnValue[i] == "blankValue")
                            requestDict[requestID].column[i] = "Fact";
                    }

                    if (requestDict[requestID].measurement == null || requestDict[requestID].measurement.Count == 0)
                    {
                        List<string> addMeasure = new List<string>();
                       // addMeasure.Add("Fact");
                        requestDict[requestID].measurement = addMeasure;
                       // responseDict[requestID].updateMeasure = true;
                        ExportHTML currentExport = new ExportHTML();
                        screenControl.Add(requestID, new Dictionary<string, StringBuilder>());
                        screenControl[requestID].Add("measurement", new StringBuilder());
                        currentExport.ramdistint2Measurement(isResponse, requestID2SessionID, wsServerSessionID, userPreference, outputFolder, requestID, clientSessionVariable, requestDict,  responseDict, ramK2V[requestDict[requestID].importFile]);
                    }
                    if (requestDict[requestID].distinctDimension == null)
                    {
                        List<string> addDistinct = new List<string>();
                        addDistinct.Add(ramKey2Valuegz[0][0].ToString());
                        requestDict[requestID].distinctDimension = addDistinct;
                       // responseDict[requestID].updateDistinct = true;
                        ExportHTML currentExport = new ExportHTML();
                        screenControl.Add(requestID, new Dictionary<string, StringBuilder>());
                        screenControl[requestID].Add("displaySelectedColumn", new StringBuilder());
                        currentExport.ramdistint2DisplaySelectedColumn(isResponse, requestID2SessionID, wsServerSessionID, userPreference, tableFact, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, ramK2V[requestDict[requestID].importFile], addDistinct);
                    }
                    else // is crosstabDimension = distinctDimension
                    {
                        bool isDimensionEqual = true;
                        if (requestDict[requestID].crosstabDimension != null)
                        {
                            if (requestDict[requestID].distinctDimension.Count <= requestDict[requestID].crosstabDimension.Count)
                            {
                                for (int i = 0; i < requestDict[requestID].crosstabDimension.Count; i++)
                                {
                                    if (requestDict[requestID].distinctDimension.Contains(requestDict[requestID].crosstabDimension[i]))
                                        isDimensionEqual = isDimensionEqual && true;
                                    else
                                        isDimensionEqual = isDimensionEqual && false;
                                }

                                if (isDimensionEqual == true)
                                {
                                    requestDict[requestID].crosstabDimension.RemoveAt(0);                                   
                                    ExportHTML currentExport = new ExportHTML();

                                    if (!screenControl.ContainsKey(requestID))
                                        screenControl.Add(requestID, new Dictionary<string, StringBuilder>());

                                    screenControl[requestID].Add("displayCrosstalDimension", new StringBuilder());
                                    currentExport.ramdistint2Crosstab(isResponse, requestID2SessionID, wsServerSessionID, userPreference, tableFact, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, ramK2V[requestDict[requestID].importFile]);
                                }
                            }
                        }
                    }
                }              

                currentReport.runReportByCurrentDataset(isResponse, requestID2SessionID, wsServerSessionID, ramMapping, ramDetail, ramK2V, userPreference, tableFact, sourceFolder, db1Folder, screenControl, currentRequestID, ramValue2Keygz, outputFolder, csvWriteSeparator, requestID, clientSessionVariable, requestDict, responseDict, columnName2ID, htmlTable, ramDetailgz, ramKey2Valuegz);                
            }
        }
        public void importFile(ConcurrentDictionary<string, bool> isResponse, string errorMessageLog, char csvWriteSeparator, Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, int> copyMemDetailRandomly, string outputFolder, ConcurrentDictionary<decimal, clientMachine.response> responseDict, string[] readFile, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, string sourceFolder, byte csvReadSeparator, string db1Folder, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, List<double>> ramDetailgz)
        {
            ImportCSV currentImport = new ImportCSV();
            StringBuilder errorMessage = new StringBuilder();
            bool currentError = false;
            errorMessage.AppendLine("Error Description");

            responseDict[requestID].extractFromCSV = false;

            if (responseDict[requestID].sendErrorMessage == false)
            {
                DateTime startTime = DateTime.Now;
                (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> ramK2Vnew, Dictionary<string, int> csvInfo) = currentImport.CSV2ramDetail(errorMessageLog, ramMapping, ramDetail, ramK2V, outputFolder, csvWriteSeparator, requestID, clientSessionVariable, requestDict, responseDict, userPreference, readFile, csvReadSeparator);

                Dictionary<int, Dictionary<double, double>> ramMappingNew = new Dictionary<int, Dictionary<double, double>>();

                responseDict[requestID].isMapping = false;
                for (int i = 0; i < ramK2Vnew.Count; i++)
                {
                    if (ramK2Vnew[i][0].ToUpper().Contains("@KEY"))
                    {
                        responseDict[requestID].isMapping = true;
                    }
                }

                if (responseDict[requestID].isMapping == true)
                {
                    for (int i = 1; i < ramDetailnew.Count; i++)
                    {
                        ramMappingNew.Add(i, new Dictionary<double, double>());

                        for (int j = 0; j < ramDetailnew[0].Count; j++)
                            ramMappingNew[i].Add(ramDetailnew[0][j], ramDetailnew[i][j]); // ( source column as key ~ target column as value ) * i pairs                      
                    }
                }

                if (requestDict[requestID].debugOutput == "Y")
                { 
                    if (responseDict[requestID].isMapping == true)
                    {
                        foreach (var pair in ramMappingNew)
                            Console.WriteLine("pairMasterK2V count " + pair.Key + " " + ramMappingNew[pair.Key].Count);             
                    }
                }

                for (int i = 0; i < ramK2Vnew.Count; i++)
                {                    
                    if ((ramK2Vnew[i].Count - 1) > userPreference["system"].maxUniqueTextForEachDimension)
                    {                            
                        errorMessage.AppendLine("\"Column name " + (char)39 + ramK2Vnew[i][0] + (char)39 + " of the import file " + (char)39 + requestDict[requestID].importFile + (char)39 + " has unique sets of data value of "  + string.Format("{0:#,0}", (ramK2Vnew[i].Count - 1)) + " which is greater than current user setting limit of " + string.Format("{0:#,0}", userPreference["system"].maxUniqueTextForEachDimension) + ".\"");                     
                        currentError = true;
                    }                   
                }

                if (currentError == true)
                {
                    responseDict[requestID].sendErrorMessage = true;
                    responseDict[requestID].errorMessage = errorMessage;
                }

                if (responseDict[requestID].sendErrorMessage == false)
                { 
                    DateTime endTime = DateTime.Now;
                    Console.WriteLine("Extract Data File " + requestDict[requestID].importFile + " Byte:" + string.Format("{0:#,0}", csvInfo["Byte"]).ToString() + " Column:" + string.Format("{0:#,0}", csvInfo["Column"]).ToString() + " Row:" + string.Format("{0:#,0}", csvInfo["Row"]).ToString() + " Time:" + String.Format("{0:F3}", (endTime - startTime).TotalSeconds) + " seconds");
                    
                    if(userPreference["system"].saveTable2Memory == false)
                    { 
                        ramK2V.Clear();
                        ramDetail.Clear();
                        if (responseDict[requestID].isMapping == true)
                            ramMapping.Clear();
                    }

                    if (ramK2V.ContainsKey(requestDict[requestID].importFile))
                    {
                        ramDetail.Remove(requestDict[requestID].importFile);
                        ramK2V.Remove(requestDict[requestID].importFile);

                        if (responseDict[requestID].isMapping == true)
                            ramMapping.Remove(requestDict[requestID].importFile);
                    }

                    if (!ramK2V.ContainsKey(requestDict[requestID].importFile))
                    {
                        ramDetail.Add(requestDict[requestID].importFile, new Dictionary<int, List<double>>());
                        ramK2V.Add(requestDict[requestID].importFile, new Dictionary<int, Dictionary<double, string>>());

                        if (responseDict[requestID].isMapping == true)
                            ramMapping.Add(requestDict[requestID].importFile, new Dictionary<int, Dictionary<double, double>>());
                    }

                    for (int i = 0; i < ramDetailnew.Count; i++)
                        ramDetail[requestDict[requestID].importFile].Add(i, ramDetailnew[i]);

                    for (int i = 0; i < ramK2Vnew.Count; i++)
                        ramK2V[requestDict[requestID].importFile].Add(i, ramK2Vnew[i]);

                    if (responseDict[requestID].isMapping == true)
                    {
                        foreach (var pair in ramMappingNew)
                            ramMapping[requestDict[requestID].importFile].Add(pair.Key, ramMappingNew[pair.Key]);
                    }
                }
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (responseDict[requestID].isMapping == true)
                {
                    foreach (var file in ramMapping)
                    {
                        Console.WriteLine("ramMapping " + file.Key + " " + ramMapping[file.Key].Count);

                        foreach (var mappingTable in ramMapping[file.Key])
                            Console.WriteLine("file.Key " + file.Key + " mappingTable.Key " + mappingTable.Key + " ramMapping[file.Key].Count " + ramMapping[file.Key].Count + " ramMapping[file.Key][mappingTable.Key].Count " + ramMapping[file.Key][mappingTable.Key].Count);

                    }
                }
            }


            if (responseDict[requestID].sendErrorMessage == true)
            {
                (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> ramK2Vnew, Dictionary<string, int> csvInfo) = currentImport.outputErrorMessage(errorMessageLog, requestID, clientSessionVariable, requestDict,  responseDict, userPreference, readFile, csvReadSeparator);

                if (ramK2V.ContainsKey(requestDict[requestID].importFile))
                {
                    ramDetail.Remove(requestDict[requestID].importFile);
                    ramK2V.Remove(requestDict[requestID].importFile);
                }

                if (!ramK2V.ContainsKey(requestDict[requestID].importFile))
                {
                    ramDetail.Add(requestDict[requestID].importFile, new Dictionary<int, List<double>>());
                    ramK2V.Add(requestDict[requestID].importFile, new Dictionary<int, Dictionary<double, string>>());
                }

                for (int i = 0; i < ramDetailnew.Count; i++)
                    ramDetail[requestDict[requestID].importFile].Add(i, ramDetailnew[i]);

                for (int i = 0; i < ramK2Vnew.Count; i++)
                    ramK2V[requestDict[requestID].importFile].Add(i, ramK2Vnew[i]);
            }        
         
            int totalColumn = ramK2V[requestDict[requestID].importFile].Count;

            if (copyMemDetailRandomly.ContainsKey(requestDict[requestID].importFile))
            {
                Simulation currentRandomReport = new Simulation();
                currentRandomReport.randomCopyTableMultiThread(copyMemDetailRandomly, totalColumn, ramDetail, ramK2V, clientSessionVariable, requestDict,  requestID);
            }

            if (userPreference["system"].saveTable2Disk == true)
            {

                if (!Directory.Exists(sourceFolder + "importedFile" + userPreference["system"].slash + requestDict[requestID].importFile.ToString() + userPreference["system"].slash))
                Directory.CreateDirectory(sourceFolder + "importedFile" + userPreference["system"].slash + requestDict[requestID].importFile.ToString() + userPreference["system"].slash);

                int fileNo = 1;
                bool fileExist = false;

                do
                {
                    fileExist = false;
                    string[] targetFile = Directory.GetFiles(sourceFolder + "importedFile" + userPreference["system"].slash + requestDict[requestID].importFile.ToString() + userPreference["system"].slash, "imported" + fileNo.ToString() + "~" + requestDict[requestID].importFile.ToString());

                    foreach (string filePath in targetFile)
                    {
                        if (filePath == sourceFolder + "importedFile" + userPreference["system"].slash + requestDict[requestID].importFile.ToString() + userPreference["system"].slash + "imported" + fileNo.ToString() + "~" + requestDict[requestID].importFile.ToString())
                        {
                            fileNo++;
                            fileExist = true;
                        }
                        else
                            fileExist = false;
                    }

                } while (fileExist == true);

               if(responseDict[requestID].uploadDesktopFile == false)
                   if(requestDict[requestID].importFile != "errorMessage.csv")
                       File.Move(sourceFolder + requestDict[requestID].importFile.ToString(), sourceFolder + "importedFile" + userPreference["system"].slash + requestDict[requestID].importFile.ToString() + userPreference["system"].slash + "imported" + fileNo.ToString() + "~" + requestDict[requestID].importFile.ToString());

            
                ConcurrentDictionary<decimal, Thread> ioThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage queue job             

                try // new a thread to manage queue job
                {
                    ioThread.TryAdd(requestID, new Thread(() => ramDB2Disk()));
                    ioThread[requestID].Start();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"queueThread fail '{e}'");
                }
            }

            isResponse[responseDict[requestID].serverSession] = true;

            void ramDB2Disk() //Backup DB to Disk                        
            {
                if (!Directory.Exists(db1Folder + userPreference["system"].slash + requestDict[requestID].importFile + userPreference["system"].slash))
                    Directory.CreateDirectory(db1Folder + userPreference["system"].slash + requestDict[requestID].importFile + userPreference["system"].slash);

                BinaryFormatter formatter = new BinaryFormatter();

                using (MemoryStream MemoryStream = new MemoryStream())               
                
                using (GZipStream gZipStream = new GZipStream(File.OpenWrite(db1Folder + userPreference["system"].slash + requestDict[requestID].importFile + userPreference["system"].slash + "ramDetail" + ".db"), CompressionMode.Compress))
                    formatter.Serialize(gZipStream, ramDetail[requestDict[requestID].importFile]);
               
                using (GZipStream gZipStream = new GZipStream(File.OpenWrite(db1Folder + userPreference["system"].slash + requestDict[requestID].importFile + userPreference["system"].slash + "ramKey2Value.db"), CompressionMode.Compress))
                    formatter.Serialize(gZipStream, ramK2V[requestDict[requestID].importFile]);
            }                        
        }     
        public void importDB(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, string sourceFolder, byte csvReadSeparator, string db1Folder, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, List<double>> ramDetailgz)
        {            
            if (userPreference["system"].saveTable2Memory == false)
            {
                ramK2V.Clear();
                ramDetail.Clear();
            }           

            if (ramDetail.ContainsKey(requestDict[requestID].importFile) || ramK2V.ContainsKey(requestDict[requestID].importFile))
            {
                ramDetail.Remove(requestDict[requestID].importFile);
                ramK2V.Remove(requestDict[requestID].importFile);
            }

            BinaryFormatter formatter1 = new BinaryFormatter();

            using (GZipStream gZipStream = new GZipStream(File.OpenRead(db1Folder + requestDict[requestID].importFile.ToString() + userPreference["system"].slash + "ramDetail.db"), CompressionMode.Decompress))
                ramDetail.Add(requestDict[requestID].importFile, (Dictionary<int, List<double>>)formatter1.Deserialize(gZipStream));

            using (GZipStream gZipStream = new GZipStream(File.OpenRead(db1Folder + requestDict[requestID].importFile.ToString() + userPreference["system"].slash + "ramKey2Value.db"), CompressionMode.Decompress))
                ramK2V.Add(requestDict[requestID].importFile, (Dictionary<int, Dictionary<double, string>>)formatter1.Deserialize(gZipStream));

            if (ramDetail.ContainsKey(requestDict[requestID].importFile))
            {  
                for (int i = 0; i < ramK2V[requestDict[requestID].importFile].Count; i++)
                {
                    ramDetailgz.Add(i, ramDetail[requestDict[requestID].importFile][i]);
                    ramKey2Valuegz.Add(i, ramK2V[requestDict[requestID].importFile][i]);
                }
            }           
        }
        public void randomGenerateSelectionCriteria(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<string, string> variable, Dictionary<string, List<string>> array, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, Dictionary<string, string> columnName2ID, List<string> selectDistinctCol, Dictionary<string, Dictionary<int, List<double>>> ramDetail, List<string> displayRandomSelectedValue) // Generate Selection Criteria and Run Report
        {   
            variable.Clear();
            array.Clear();
            int countTextColumn = 0;
            int countMeasureColumn = 0;
            List<int> selectDistinctColKey = new List<int>();
            List<string> selectFilterColValue = new List<string>();
            List<string> selectMeasureCol = new List<string>();
            string currentRandomDistinctCol;


            for (int j = 0; j < ramK2V[requestDict[requestID].importFile].Count; j++)
            {
                if (ramK2V[requestDict[requestID].importFile][j][0].ToString().ToUpper() == "LEDGER")
                {
                    selectFilterColValue.Add(ramK2V[requestDict[requestID].importFile][j][0].ToString());
                    selectDistinctColKey.Add(j);
                }
            }

            if (selectDistinctColKey.Count == 0)
            {
                do
                {
                    if (ramK2V[requestDict[requestID].importFile][countTextColumn].Count > 1) // add filter column for random selection except Period Change and Period End
                    {
                        if (!(ramK2V[requestDict[requestID].importFile][countTextColumn][0].ToString() == "Period Change" || ramK2V[requestDict[requestID].importFile][countTextColumn][0].ToString() == "Period End"))
                        {
                            selectFilterColValue.Add(ramK2V[requestDict[requestID].importFile][countTextColumn][0].ToString());
                            selectDistinctColKey.Add(countTextColumn);
                        }
                    }
                    countTextColumn++;
                } while (countTextColumn < ramK2V[requestDict[requestID].importFile].Count);
            }

            do
            {
                if (ramK2V[requestDict[requestID].importFile][countMeasureColumn].Count == 1)
                {                    
                    string measurement = ramK2V[requestDict[requestID].importFile][countMeasureColumn][0].ToString().ToUpper();
                    if (measurement.Contains("AMOUNT") || measurement.Contains("QUANTITY") || measurement.Contains("QTY"))
                        selectMeasureCol.Add(ramK2V[requestDict[requestID].importFile][countMeasureColumn][0].ToString().Replace(" ", "#"));
                }
                countMeasureColumn++;
            } while (countMeasureColumn < ramK2V[requestDict[requestID].importFile].Count);

            if(selectMeasureCol.Count == 0)
            {
                for (int j = 0; j < ramK2V[requestDict[requestID].importFile].Count; j++)
                    if (ramK2V[requestDict[requestID].importFile][j][0].ToString().ToUpper() == "FACT")
                        selectMeasureCol.Add(ramK2V[requestDict[requestID].importFile][j][0].ToString());
            }
           
            if (responseDict.ContainsKey(requestID))
                clientSessionVariable[responseDict[requestID].serverSession].clientSessionMeasurementName = selectMeasureCol;

            Random randomValue = new Random();
            int selectedFilter = randomValue.Next(0, selectDistinctColKey.Count - 1);
            int selectedFromToValue = randomValue.Next(1, ramK2V[requestDict[requestID].importFile][selectDistinctColKey[selectedFilter]].Count - 1);
            var selectedFilterCol = selectFilterColValue[selectedFilter].ToString().Trim().Replace(" ", "#");
            var selectedFilterCol2 = selectFilterColValue[selectedFilter].ToString().Trim();

            List<string> sortedValue = new List<string>();
            string addColumnID = columnName2ID[selectedFilterCol2];

            bool success = Int32.TryParse(addColumnID, out int number);
            for (int j = 1; j < ramK2V[requestDict[requestID].importFile][number].Count; j++)
                sortedValue.Add(ramK2V[requestDict[requestID].importFile][number][j]);

            sortedValue.Sort();

            var selectedFilterColValue = ramK2V[requestDict[requestID].importFile][selectDistinctColKey[selectedFilter]][selectedFromToValue].ToString();      

            var selectedStartFilterColValue = sortedValue[0].Replace(":", ";");
            selectedStartFilterColValue = selectedStartFilterColValue.Replace("/", "|");            

            var selectedEndFilterColValue = sortedValue[sortedValue.Count - 1].Replace(":", ";");
            selectedEndFilterColValue = selectedEndFilterColValue.Replace("/", "|");
          
            int loop = 0;

            for (int j = 0; j < ramK2V[requestDict[requestID].importFile].Count; j++)
            {
                if (ramK2V[requestDict[requestID].importFile][j][0].ToString().ToUpper() == "LEDGER" || ramK2V[requestDict[requestID].importFile][j][0].ToString().ToUpper() == "ACCOUNT")
                {
                    selectDistinctCol.Add(ramK2V[requestDict[requestID].importFile][j][0].ToString());                    
                }
            }

            if (selectDistinctCol.Count == 0)
            {
                do
                {
                    var currentRandomCol = randomValue.Next(0, selectDistinctColKey.Count);
                    currentRandomDistinctCol = ramK2V[requestDict[requestID].importFile][selectDistinctColKey[currentRandomCol]][0];

                    if (!(currentRandomDistinctCol.Contains("Period End") || currentRandomDistinctCol.Contains("Period Change")))
                    {
                        if (!selectDistinctCol.Contains(currentRandomDistinctCol.Replace(" ", "#")))
                        {
                            if (ramK2V[requestDict[requestID].importFile][selectDistinctColKey[currentRandomCol]].Count > 1)
                                selectDistinctCol.Add(currentRandomDistinctCol.Replace(" ", "#"));
                        }
                    }
                    loop++;
                    if (loop > 10) break;

                } while (selectDistinctCol.Count <= selectDistinctColKey.Count && selectDistinctCol.Count < 3);
            }

            StringBuilder findAutoRequest = new StringBuilder();

            if (ramDetail[requestDict[requestID].importFile][0].Count > 200000)
            {
                findAutoRequest.Append("{\"processID\":\"runReport\",\"processButton\": \"drillRow\",\"userID\":\"system\",\"debugOutput\":\"N\",\"pageXlength\":\"20\",\"pageYlength\":\"16\",\"sortingOrder\":\"sortAscending\",\"sortXdimension\":\"A\",\"sortYdimension\":\"A\",\"precisionLevel\":\"Dollar\",\"measureType\":\"sum\",\"column\":[" + selectedFilterCol + "],\"startOption\":[\">=\"], \"startColumnValue\":[" + selectedFilterColValue.ToString() + "],\"endOption\":[\"<=\"],\"endColumnValue\":[" + selectedFilterColValue.ToString() + "],\"distinctDimension\":[");
                selectedStartFilterColValue = selectedFilterColValue.ToString();
                selectedEndFilterColValue = selectedFilterColValue.ToString();
            }
            else
                findAutoRequest.Append("{\"processID\":\"runReport\",\"processButton\": \"drillRow\",\"userID\":\"system\",\"debugOutput\":\"N\",\"pageXlength\":\"20\",\"pageYlength\":\"16\",\"sortingOrder\":\"sortAscending\",\"sortXdimension\":\"A\",\"sortYdimension\":\"A\",\"precisionLevel\":\"Dollar\",\"measureType\":\"sum\",\"column\":[" + selectedFilterCol + "],\"startOption\":[\">=\"], \"startColumnValue\":[" + selectedStartFilterColValue.ToString() + "],\"endOption\":[\"<=\"],\"endColumnValue\":[" + selectedEndFilterColValue.ToString() + "],\"distinctDimension\":[");

            displayRandomSelectedValue.Add(selectedFilterCol);
            displayRandomSelectedValue.Add(selectedStartFilterColValue.ToString());
            displayRandomSelectedValue.Add(selectedEndFilterColValue.ToString());

            for (int i = 0; i < selectDistinctCol.Count; i++)            
            {
                findAutoRequest.Append(selectDistinctCol[i].ToString());

                if (selectDistinctCol.Count > 1 && i < selectDistinctCol.Count - 1)
                    findAutoRequest.Append(",");
            }

            findAutoRequest.Append("],\"crosstabDimension\":[],\"measurement\":[");

            for (int i = 0; i < selectMeasureCol.Count; i++)
            {
                findAutoRequest.Append(selectMeasureCol[i].ToString());

                if (selectMeasureCol.Count > 1 && i < selectMeasureCol.Count - 1)
                    findAutoRequest.Append(",");
            }

            findAutoRequest.Append("],\"cancelRequestID\":" + requestID.ToString() + "}");           

            string autoRequest = findAutoRequest.ToString();          

            Json convert = new Json();
            convert.Json2VariableArray(autoRequest);
            convert.Json2VariableList(autoRequest, requestID, requestDict);
        }        
        public void runReportByCurrentDataset(ConcurrentDictionary<string, bool> isResponse, ConcurrentDictionary<decimal, IWebSocketConnection> requestID2SessionID, List<IWebSocketConnection> wsServerSessionID, Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, string sourceFolder, string db1Folder, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, ConcurrentDictionary<int, decimal> currentRequestID, Dictionary<int, Dictionary<string, double>> ramValue2Keygz, string outputFolder, char csvWriteSeparator, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<string, string> _columnName2ID, Dictionary<int, StringBuilder> htmlTable, Dictionary<int, List<double>> _ramDetailgz, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz)
        {
            List<string> addCrosstab2Distinct = new List<string>();

            for (int i = 0; i < _ramKey2Valuegz.Count; i++)
            {
                for (int j = 0; j < requestDict[requestID].distinctDimension.Count; j++)
                {                  
                    if (_ramKey2Valuegz[i][0] == requestDict[requestID].distinctDimension[j].Trim().Replace("#", " "))
                    {                       
                        if(!addCrosstab2Distinct.Contains(_ramKey2Valuegz[i][0]))
                            addCrosstab2Distinct.Add(_ramKey2Valuegz[i][0]);                      
                    }
                }

                if (requestDict[requestID].crosstabDimension != null)
                {
                    for (int j = 0; j < requestDict[requestID].crosstabDimension.Count; j++)
                        if (_ramKey2Valuegz[i][0] == requestDict[requestID].crosstabDimension[j].Trim().Replace("#", " "))
                        {
                            if (!addCrosstab2Distinct.Contains(_ramKey2Valuegz[i][0]))
                                addCrosstab2Distinct.Add(_ramKey2Valuegz[i][0]);
                        }
                }
            }

            requestDict[requestID].distinctDimension.Clear();

            for (int i = 0; i < addCrosstab2Distinct.Count; i++)
            {
               // Console.WriteLine("addCrosstab2Distinct[i] " + addCrosstab2Distinct[i]);
                requestDict[requestID].distinctDimension.Add(addCrosstab2Distinct[i]);
            }

            Dictionary<string, int> selectedColumn = new Dictionary<string, int>();

            for (int i = 0; i < requestDict[requestID].distinctDimension.Count; i++)
                selectedColumn.Add(requestDict[requestID].distinctDimension[i].Trim().Replace("#", " "), i);           

            int matchColumn = 0;
            if (requestDict[requestID].distinctOrder != null)
                if(requestDict[requestID].distinctOrder.Count == requestDict[requestID].distinctDimension.Count)
                {
                    for (int i = 0; i < requestDict[requestID].distinctOrder.Count; i++)                        
                        if (selectedColumn.ContainsKey(requestDict[requestID].distinctOrder[i].Trim().Replace("#", " ").ToString()))
                            matchColumn++;

                    if(matchColumn == requestDict[requestID].distinctOrder.Count)
                    {
                        selectedColumn.Clear();
                        for (int i = 0; i < requestDict[requestID].distinctOrder.Count; i++)                      
                            selectedColumn.Add(requestDict[requestID].distinctOrder[i].Trim().Replace("#", " ").ToString(), i);
                    }
                } 
            
            List<string> reorganisedColumn = new List<string>(); // move selected distinct column to 0,1,2 order
            List<string> sourceColumn1 = new List<string>();
            List<string> sourceColumn2 = new List<string>();  

            for (int i = 0; i < _ramKey2Valuegz.Count; i++)
                if (selectedColumn.ContainsKey(_ramKey2Valuegz[i][0]))
                    sourceColumn1.Add(_ramKey2Valuegz[i][0]);               
                else
                    sourceColumn2.Add(_ramKey2Valuegz[i][0]);

            foreach (var pair in selectedColumn)
                reorganisedColumn.Add(pair.Key);

            for (int i = 0; i < sourceColumn2.Count; i++)
                reorganisedColumn.Add(sourceColumn2[i]);

            StringBuilder debug = new StringBuilder();                       
            Dictionary<int, Dictionary<double, string>> ramKey2Valuegz = new Dictionary<int, Dictionary<double, string>>();          
            Dictionary<string, string> columnName2ID = new Dictionary<string, string>();
            Dictionary<int, List<double>> ramDetailgz = new Dictionary<int, List<double>>();                      

            for (int i = 0; i < reorganisedColumn.Count; i++)
                for (int j = 0; j < _ramKey2Valuegz.Count; j++)
                    if (reorganisedColumn[i] == _ramKey2Valuegz[j][0])
                    {                        
                        ramDetailgz[i] = _ramDetailgz[j];
                        ramKey2Valuegz.Add(i, new Dictionary<double, string>());
                        foreach (var pair in _ramKey2Valuegz[j])                        
                            ramKey2Valuegz[i].Add(pair.Key, pair.Value); 
                    }

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
                columnName2ID.Add(ramKey2Valuegz[i][0], i.ToString());

            // to validate user confirmed dimension and its criteria; if error for certain cases, will assign factory setting
            SelectionCriteria currentSelection = new SelectionCriteria();  
            (Dictionary<int, List<string>> selectionCriteria, List<int> distinctDimension, List<int> crosstabDimension) = currentSelection.validateSelectionCrieteria(requestID, clientSessionVariable, requestDict,  responseDict, columnName2ID, ramDetailgz, ramKey2Valuegz);

            // to sort and group column ID based on selected X Y dimension (by X, by Y and by XY)
            (List<int> yDimension, Dictionary<int, string> xyDimension, List<int> revisedX, List<int> revisedY) = currentSelection.sortSelectedDimensionOrder(selectionCriteria, distinctDimension, requestID, clientSessionVariable, requestDict,  debug, crosstabDimension);

            // selected code range of particular dimension   
            Dictionary<int, Dictionary<double, string>> dimensionCriteria = new Dictionary<int, Dictionary<double, string>>();                      
            dimensionCriteria = currentSelection.conditional2ExactMatch(ramDetailgz, ramKey2Valuegz, selectionCriteria);        

            // recordMeasurementSelectionCriteria for number fitering
            (Dictionary<int, List<int>> measurementColumn, Dictionary<int, List<string>> measurementOperator, Dictionary<int, List<double>> measurementRange, List<int> measure, bool noMultipleMeasurementColumn) = currentSelection.recordMeasurementSelectionCriteria(requestID, clientSessionVariable, requestDict,  selectionCriteria, crosstabDimension, ramKey2Valuegz, columnName2ID);

            // filter => distinct => drilldown
            Distinct currentDistinct = new Distinct();
            (Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<decimal, List<int>> distinctList2DrillDown, List<decimal> distinctListChecksum, Dictionary<decimal, int> distinctDimensionChecksumList) = currentDistinct.filterDistinctDrillDown(csvWriteSeparator, outputFolder, debug, userPreference, ramDetailgz, distinctDimension, measure, requestID, clientSessionVariable, requestDict,  ramKey2Valuegz, responseDict, measurementColumn, noMultipleMeasurementColumn, dimensionCriteria, crosstabDimension, measurementOperator, measurementRange);
            List<int> periodChangeColumn = new List<int>();

            // calc balance
            responseDict[requestID].periodExist = false;
            for (int i = 0; i < distinctRamKey2Value.Count; i++)
            {
                if (distinctRamKey2Value[i][0].ToString().ToUpper() == "PERIOD END")
                {
                    responseDict[requestID].periodChangeColumn = i;                  
                    responseDict[requestID].periodExist = true;
                }
            }
            
            if (responseDict[requestID].periodExist == true &&  !requestDict[requestID].importFile.Contains("BalanceTable"))
            {
                var start = DateTime.Now;
                ComputeRow currentComputeRow = new ComputeRow();
                (Dictionary<int, List<double>> ramDetailBalTable, Dictionary<int, Dictionary<double, string>> remKey2ValueBalTable) = currentComputeRow.calcBalance(userPreference, outputFolder, csvWriteSeparator, requestID, clientSessionVariable, requestDict,  responseDict, distinctRamKey2Value, distinctList);              

                if (ramDetailBalTable.Count > 0)
                {
                    string revisedImportFile = "BalanceTable-" + requestDict[requestID].importFile;

                    if (ramDetail.ContainsKey(revisedImportFile))
                        ramDetail.Remove(revisedImportFile);

                    if (ramK2V.ContainsKey(revisedImportFile))
                        ramK2V.Remove(revisedImportFile);

                    ramDetail.Add(revisedImportFile, new Dictionary<int, List<double>>());
                    ramK2V.Add(revisedImportFile, new Dictionary<int, Dictionary<double, string>>());    

                    for (int i = 0; i < ramDetailBalTable.Count; i++)
                        ramDetail[revisedImportFile].Add(i, ramDetailBalTable[i]);

                    for (int i = 0; i < remKey2ValueBalTable.Count; i++)
                        ramK2V[revisedImportFile].Add(i, remKey2ValueBalTable[i]);
                   
                    if (requestDict[requestID].crosstabDimension != null && requestDict[requestID].crosstabDimension.Contains("Period#End"))                   
                        requestDict[requestID].crosstabDimension = requestDict[requestID].crosstabDimensionTemp;

                    requestDict[requestID].importFile = revisedImportFile;
                    responseDict[requestID].calcBalance = true;                    
                }

                var end = DateTime.Now;
                Console.WriteLine("Calc Balance Time " + String.Format("{0:F3}", (end - start).TotalSeconds) + " seconds");
            }

            if (responseDict[requestID].calcBalance == false)
            {
                // interim event
                Dictionary<decimal, decimal> unsorted2SortedCheksum = new Dictionary<decimal, decimal>();
                List<decimal> distinctSet = new List<decimal>();
                Dictionary<int, string> dataSortingOrder = new Dictionary<int, string>();
                dataSortingOrder[0] = "sortAscending";
                Dictionary<int, string> columnMoveOrder = new Dictionary<int, string>();
                int xAddressCol = distinctList.Count; // to store x coordinate of crosstab
                int yAddressCol = distinctList.Count + 1; // to store y coordinate of crosstab            

                Dictionary<int, Dictionary<double, double>> ramKey2Order = new Dictionary<int, Dictionary<double, double>>();
                Dictionary<int, Dictionary<double, double>> ramOrder2Key = new Dictionary<int, Dictionary<double, double>>();
                int order = 0; // sorting based on reorganised and distinct dimension
                for (int i = 0; i < distinctDimension.Count; i++)
                {
                    order = 0;
                    ramKey2Order.Add(distinctDimension[i], new Dictionary<double, double>());
                    ramOrder2Key.Add(distinctDimension[i], new Dictionary<double, double>());
                    foreach (var item in ramKey2Valuegz[distinctDimension[i]].OrderBy(j => j.Value))
                    {
                        order++;
                        ramKey2Order[distinctDimension[i]].Add(item.Key, order);
                        ramOrder2Key[distinctDimension[i]].Add(order, item.Key);
                    }
                }

                ConcurrentDictionary<decimal, Thread> presentationThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage presentation
                Dictionary<int, int> stopPresentationThread = new Dictionary<int, int>();
                int presentationJob = 0;
                int moveColumnID = 0;
                string startRotateDimension = "N";
                string startMoveDimension = "N";

                responseDict[requestID].distinctCount = Convert.ToDecimal(distinctList[0].Count) - 1;

                if (responseDict[requestID].distinctCount <= userPreference["system"].maxDistinctSet)
                {
                    presentationJob++;

                    try
                    {
                        Presentation newPresentation = new Presentation();
                        presentationThread.TryAdd(presentationJob, new Thread(() => newPresentation.presentationOfData(isResponse, requestID2SessionID, wsServerSessionID, ramDetailgz, userPreference, tableFact, ramK2V, sourceFolder, db1Folder, screenControl, distinctDimensionChecksumList, distinctListChecksum, distinctSet, unsorted2SortedCheksum, distinctDimension, xyDimension, crosstabDimension, yDimension, _ramKey2Valuegz, ramKey2Valuegz, revisedX, revisedY, requestID, clientSessionVariable, requestDict,  debug, outputFolder, responseDict, distinctRamKey2Value, ramKey2Order, ramOrder2Key, distinctList, htmlTable, measure, csvWriteSeparator, dataSortingOrder, xAddressCol, yAddressCol, presentationJob, stopPresentationThread, presentationThread, columnMoveOrder, moveColumnID, startRotateDimension, columnName2ID, startMoveDimension, distinctList2DrillDown)));
                        presentationThread[presentationJob].Start();
                        stopPresentationThread[presentationJob] = presentationThread.Count;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"presentationThread fail '{e}'");
                    }

                    InterimEvent currentEvent = new InterimEvent();
                    currentEvent.postDistinctEvent(isResponse, requestID2SessionID, wsServerSessionID, ramDetailgz, userPreference, tableFact, ramK2V, sourceFolder, db1Folder, screenControl, distinctDimensionChecksumList, currentRequestID, distinctListChecksum, distinctSet, unsorted2SortedCheksum, distinctDimension, xyDimension, requestID, clientSessionVariable, requestDict,  responseDict, presentationJob, dataSortingOrder, presentationThread, crosstabDimension, yDimension, _ramKey2Valuegz, ramKey2Valuegz, revisedX, revisedY, debug, outputFolder, distinctRamKey2Value, stopPresentationThread, ramKey2Order, ramOrder2Key, distinctList, htmlTable, measure, csvWriteSeparator, xAddressCol, yAddressCol, columnName2ID, columnMoveOrder, startRotateDimension, startMoveDimension, distinctList2DrillDown);
                }
                else
                {
                    StringBuilder errorMessage = new StringBuilder();
                    errorMessage.AppendLine("Error Description");
                    errorMessage.AppendLine("\"Selected dimensions of the filtered list " + (char)39 + requestDict[requestID].importFile + (char)39 + " has calculated distinct sets of " + string.Format("{0:#,0}", responseDict[requestID].distinctCount) + " which is greater than current user setting limit of " + string.Format("{0:#,0}", userPreference["system"].maxDistinctSet) + ".\"");
                    responseDict[requestID].errorMessage = errorMessage;
                    responseDict[requestID].sendErrorMessage = true;
                }
            }
        }
    }       
}