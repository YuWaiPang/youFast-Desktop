using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFast
{
    public class ImportCSV
    {
        public (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> remK2Vnew, Dictionary<string, int> csvInfo) CSV2ramDetail(string errorMessageLog, Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, string outputFolder, char csvWriteSeparator, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, string[] readFile, byte csvReadSeparator)
        {
            responseDict[requestID].extractFromCSV = true;

            Dictionary<int, List<int>> byteAddress = new Dictionary<int, List<int>>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            List<int> abc123Segment = new List<int>();
            StringBuilder errorMessage = new StringBuilder();           
            StringBuilder filePath = new StringBuilder();
            byte separator = csvReadSeparator;
            int openCloseDoubleQuote = 1;          

            foreach (string value in readFile)
                filePath.Append(value);           

            byte[] abc123 = File.ReadAllBytes(filePath.ToString());

            int csvLength = abc123.Length;

            if (abc123[csvLength - 1] != 13 && abc123[csvLength - 1] != 10) // check exist of 10 and/or 13 at the end of file
            {
                csvLength = csvLength + 1;
                Array.Resize(ref abc123, csvLength);
                abc123[abc123.GetUpperBound(0)] = 13;
                csvLength = csvLength + 1;
                Array.Resize(ref abc123, csvLength);
                abc123[abc123.GetUpperBound(0)] = 10;
            }

            if (csvLength > userPreference["system"].maxImportFileByte)
            {
                errorMessage.Clear();
                errorMessage.AppendLine("overMaxImportFileByteSize");
                errorMessage.AppendLine("\"Import file " + (char)39 + requestDict[requestID].importFile + (char)39 + " has " + string.Format("{0:#,0}", abc123.Length) + " bytes which is greater than user perference limit of " + string.Format("{0:#,0}", userPreference["system"].maxImportFileByte) + " bytes\"");
                Array.Clear(abc123, 0, abc123.Length);

                if (!Directory.Exists(errorMessageLog))
                    Directory.CreateDirectory(errorMessageLog);

                using (StreamWriter toDisk = new StreamWriter(errorMessageLog + userPreference["system"].slash + "errorMessage.csv"))
                {
                    toDisk.Write(errorMessage);
                    toDisk.Close();
                }
                requestDict[requestID].importFile = "errorMessage.csv";
                abc123 = File.ReadAllBytes(errorMessageLog + userPreference["system"].slash + "errorMessage.csv");
                csvLength = abc123.Length;
            }

            ImportCSV currentColumn1 = new ImportCSV();
            (bool useDoubleQuote, int maxColumn, int nextRowChar, List<int> nextRow) = currentColumn1.determineMaxColumn(abc123, csvReadSeparator);           

            for (int i = 0; i <= maxColumn; i++)
                byteAddress.Add(i, new List<int>());    

            if (csvLength > 100000 && userPreference["system"].maxSegmentThread > 1)
            {
                ImportCSV multiSegment = new ImportCSV();
                multiSegment.multiThreadBySegmentAddress(userPreference, csvLength, abc123Segment, checkSegmentThreadCompleted, byteAddress, abc123, nextRow, useDoubleQuote, openCloseDoubleQuote, maxColumn, nextRowChar, separator, csvReadSeparator);
            }
            else
            {
                abc123Segment.Add(0);
                abc123Segment.Add(csvLength);
                ImportCSV singleSegment = new ImportCSV();
                byteAddress = singleSegment.findAddress(checkSegmentThreadCompleted, 0, abc123Segment, useDoubleQuote, abc123, maxColumn, nextRow, nextRowChar, separator, csvReadSeparator, openCloseDoubleQuote);
            }

            byteAddress[0].RemoveAt(byteAddress[0].Count - 1);
           
            if (byteAddress.Count > userPreference["system"].maxImportFileColumn || byteAddress[0].Count > userPreference["system"].maxImportFileRow)
            {
                errorMessage.Clear();
                errorMessage.AppendLine("Error Description");

                if (byteAddress.Count > userPreference["system"].maxImportFileColumn)
                    errorMessage.AppendLine("Import file " + (char)39 + requestDict[requestID].importFile + (char)39 + " has " + byteAddress.Count + " columns which is greater than current user setting limit of " + userPreference["system"].maxImportFileColumn + ".");

                if (byteAddress[0].Count > userPreference["system"].maxImportFileRow)
                    errorMessage.AppendLine("\"Import file " + (char)39 + requestDict[requestID].importFile + (char)39 + " has " +  string.Format("{0:#,0}", byteAddress[0].Count) + " rows which is greater than current user setting limit of " + string.Format("{0:#,0}", userPreference["system"].maxImportFileRow) + ".\"");

                Array.Clear(abc123, 0, abc123.Length);

                if (!Directory.Exists(errorMessageLog))
                    Directory.CreateDirectory(errorMessageLog);

                using (StreamWriter toDisk = new StreamWriter(errorMessageLog + userPreference["system"].slash + "errorMessage.csv"))
                {
                    toDisk.Write(errorMessage);
                    toDisk.Close();
                }

                requestDict[requestID].importFile = "errorMessage.csv";

                abc123 = File.ReadAllBytes(errorMessageLog + userPreference["system"].slash + "errorMessage.csv");
                csvLength = abc123.Length;

                ImportCSV currentColumn2 = new ImportCSV();
                (bool useDoubleQuote2, int maxColumn2, int nextRowChar2, List<int> nextRow2) = currentColumn2.determineMaxColumn(abc123, csvReadSeparator);
                useDoubleQuote = useDoubleQuote2; maxColumn = maxColumn2; nextRowChar = nextRowChar2; nextRow = nextRow2;

                byteAddress.Clear();
                for (int i = 0; i <= maxColumn; i++)
                    byteAddress.Add(i, new List<int>());

                abc123Segment.Clear();
                abc123Segment.Add(0);
                abc123Segment.Add(csvLength);

                ImportCSV singleSegment = new ImportCSV();
                byteAddress = singleSegment.findAddress(checkSegmentThreadCompleted, 0, abc123Segment, useDoubleQuote, abc123, maxColumn, nextRow, nextRowChar, separator, csvReadSeparator, openCloseDoubleQuote);
                byteAddress[0].RemoveAt(byteAddress[0].Count - 1);
            }

            ImportCSV currentColumn = new ImportCSV();
            (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> ramK2Vnew, Dictionary<string, int> csvInfo) = currentColumn.writeColumn(maxColumn, byteAddress, abc123, userPreference, csvLength);

            DateTime start = DateTime.Now;
            ComputeColumn period = new ComputeColumn();
            string AddPeriodName = "Period Change";           
            (List<double> oneRamDetail, Dictionary<double, string> oneRamK2V1) = period.computePeriod(AddPeriodName, userPreference, ramDetailnew, ramK2Vnew);
            int totalColumn = ramK2Vnew.Count;
            
            if (oneRamDetail.Count == ramDetailnew[0].Count)
            {                
                ramDetailnew.Add(totalColumn, oneRamDetail);
                ramK2Vnew.Add(totalColumn, oneRamK2V1);
            }

            AddPeriodName = "Period End";
            ComputeColumn periodEnd = new ComputeColumn();
            (oneRamDetail, oneRamK2V1) = periodEnd.computePeriod(AddPeriodName, userPreference, ramDetailnew, ramK2Vnew);
            totalColumn = ramK2Vnew.Count;
            
            if (oneRamDetail.Count == ramDetailnew[0].Count)
            {
                ramDetailnew.Add(totalColumn, oneRamDetail);
                ramK2Vnew.Add(totalColumn, oneRamK2V1);
            }

            ComputeColumn newColumn = new ComputeColumn();
            newColumn.mapColumn(userPreference, ramMapping, ramDetail, ramK2V, outputFolder, csvWriteSeparator, requestID, requestDict, responseDict, ramDetailnew, ramK2Vnew);            
            
            ComputeColumn fact = new ComputeColumn();

            bool isFactColExist = false;
            for (int i = 0; i < ramK2Vnew.Count; i++)
            {
                if (ramK2Vnew[i][0].ToString().ToUpper() == "FACT")
                    isFactColExist = true;
            }
            if (isFactColExist == false)
            {
                (oneRamDetail, oneRamK2V1) = fact.computeFact(userPreference, ramDetailnew, ramK2Vnew);
                totalColumn = ramK2Vnew.Count;
                ramDetailnew.Add(totalColumn, oneRamDetail);
                ramK2Vnew.Add(totalColumn, oneRamK2V1);
            }
           
            return (ramDetailnew, ramK2Vnew, csvInfo);           
        }
        public (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> remK2Vnew, Dictionary<string, int> csvInfo) outputErrorMessage(string errorMessageLog, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, string[] readFile, byte csvReadSeparator)
        {           
            Dictionary<int, List<int>> byteAddress = new Dictionary<int, List<int>>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            List<int> abc123Segment = new List<int>();
            StringBuilder errorMessage = new StringBuilder();            
            byte separator = csvReadSeparator;
            int openCloseDoubleQuote = 1;
            bool useDoubleQuote = false;            

            if (!Directory.Exists(errorMessageLog))
                Directory.CreateDirectory(errorMessageLog);

            using (StreamWriter toDisk = new StreamWriter(errorMessageLog + userPreference["system"].slash + "errorMessage.csv"))
            {
                toDisk.Write(responseDict[requestID].errorMessage);
                toDisk.Close();
            }

            requestDict[requestID].importFile = "errorMessage.csv";
            byte[] abc123 = File.ReadAllBytes(errorMessageLog + userPreference["system"].slash + "errorMessage.csv");

            int csvLength = abc123.Length;

            ImportCSV currentColumn2 = new ImportCSV();
            (bool useDoubleQuote2, int maxColumn, int nextRowChar, List<int> nextRow) = currentColumn2.determineMaxColumn(abc123, csvReadSeparator);
            useDoubleQuote = useDoubleQuote2;
            byteAddress.Clear();
            for (int i = 0; i <= maxColumn; i++)
                byteAddress.Add(i, new List<int>());

            abc123Segment.Clear();
            abc123Segment.Add(0);
            abc123Segment.Add(csvLength);

            ImportCSV singleSegment = new ImportCSV();
            byteAddress = singleSegment.findAddress(checkSegmentThreadCompleted, 0, abc123Segment, useDoubleQuote, abc123, maxColumn, nextRow, nextRowChar, separator, csvReadSeparator, openCloseDoubleQuote);
            byteAddress[0].RemoveAt(byteAddress[0].Count - 1);
          
            ImportCSV currentColumn = new ImportCSV();
            (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> remK2Vnew, Dictionary<string, int> csvInfo) = currentColumn.writeColumn(maxColumn, byteAddress, abc123, userPreference, csvLength);
            return (ramDetailnew, remK2Vnew, csvInfo);
        }
        public (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> remK2Vnew, Dictionary<string, int> csvInfo) writeColumn(int maxColumn, Dictionary<int, List<int>> byteAddress, byte[] abc123, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, int csvLength)
        {
            ConcurrentDictionary<int, List<double>> ramDetail = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, Dictionary<double, string>> ramK2V = new ConcurrentDictionary<int, Dictionary<double, string>>();
            ConcurrentDictionary<int, Dictionary<string, double>> ramV2K = new ConcurrentDictionary<int, Dictionary<string, double>>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            StringBuilder cellValue = new StringBuilder();
            Dictionary<string, int> csvInfo = new Dictionary<string, int>();
            ConcurrentDictionary<int, bool> isNumType = new ConcurrentDictionary<int, bool>();
            List<string> stringColumn = new List<string>();
            stringColumn.Add("DATE"); stringColumn.Add("YEAR"); stringColumn.Add("ACCOUNT");

            for (int i = 0; i < maxColumn; i++) // add column name
            {
                ramDetail.TryAdd(i, new List<double>());
                ramK2V.TryAdd(i, new Dictionary<double, string>());
                ramV2K.TryAdd(i, new Dictionary<string, double>());

                cellValue.Clear();

                for (int j = byteAddress[i][0]; j < byteAddress[i + 1][0] - 1; j++)
                    cellValue.Append((char)abc123[j]);

                string columnName = cellValue.ToString().ToUpper();
                ramK2V[i].Add(0, cellValue.ToString().Trim());
                ramV2K[i].Add(cellValue.ToString().Trim(), 0);
                ramDetail[i].Add(0);

                for (int k = 0; k < stringColumn.Count; k++)
                    if (columnName.Contains(stringColumn[k]))
                        isNumType.TryAdd(i, false);
            }

            ConcurrentDictionary<int, ImportCSV> writeColumnThread = new ConcurrentDictionary<int, ImportCSV>();

            for (int worker = 0; worker < maxColumn; worker++) 
                writeColumnThread.TryAdd(worker, new ImportCSV());          

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = userPreference["system"].maxColumnThread
            };

            Parallel.For(0, maxColumn, options, x =>
            {
                writeColumnThread[x].writeOneColumn(checkThreadCompleted, abc123, x, maxColumn, byteAddress, ramDetail, ramK2V, ramV2K, isNumType);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < maxColumn);

            Dictionary<int, List<double>> ramDetailnew = new Dictionary<int, List<double>>(ramDetail);
            Dictionary<int, Dictionary<double, string>> remK2Vnew = new Dictionary<int, Dictionary<double, string>>(ramK2V);

            csvInfo.Add("Column", maxColumn);
            csvInfo.Add("Row", byteAddress[0].Count - 1);
            csvInfo.Add("Byte", csvLength);

            return (ramDetailnew, remK2Vnew, csvInfo);
        }      
        public (bool, int, int, List<int> nextRow) determineMaxColumn(byte[] abc123, byte csvReadSeparator)
        {
            int csvLength = 0; int s = 0; int column = 0; int maxColumn = 0; int nextRowChar = 0; 
            byte separator = csvReadSeparator; int openCloseDoubleQuote = 1;
            bool useDoubleQuote = false;

            List<int> nextRow = new List<int>();            

            csvLength = abc123.Length;

            if (abc123[csvLength - 1] != 13 && abc123[csvLength - 1] != 10) // check exist of 10 and/or 13 at the end of file
            {
                csvLength = csvLength + 1;
                Array.Resize(ref abc123, csvLength);
                abc123[abc123.GetUpperBound(0)] = 13;
                csvLength = csvLength + 1;
                Array.Resize(ref abc123, csvLength);
                abc123[abc123.GetUpperBound(0)] = 10;
            }            

            do // determine max number of column
            {             
                if (abc123[s] == 34) // doubleQuote char
                {
                    useDoubleQuote = true;
                    openCloseDoubleQuote = openCloseDoubleQuote * -1;
                }

                if (openCloseDoubleQuote == 1)
                    separator = csvReadSeparator;
                else
                    separator = 127; // delete key     

                if (abc123[s] == separator)
                    column++;
                s++;
               
            } while (!(abc123[s] == 10 || abc123[s] == 13) && s < csvLength); // check first row only to determine number of column

            if (!nextRow.Contains(abc123[s]))
                nextRow.Add(abc123[s]);

            if (abc123[s + 1] == 10 || abc123[s + 1] == 13)
                if (!nextRow.Contains(abc123[s + 1]))
                    nextRow.Add(abc123[s + 1]);

            maxColumn = column + 1;            

            column = 1;
            nextRowChar = nextRow.Count; // use double quote to include separator 

            return (useDoubleQuote, maxColumn, nextRowChar, nextRow);
        }
        public void multiThreadBySegmentAddress(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, int csvLength, List<int> abc123Segment, ConcurrentQueue<int> checkSegmentThreadCompleted, Dictionary<int, List<int>> byteAddress, byte[] abc123, List<int> nextRow, bool useDoubleQuote, int openCloseDoubleQuote, int maxColumn, int nextRowChar, byte separator, byte csvReadSeparator)
        {           
            ConcurrentDictionary<int, Dictionary<int, List<int>>> tempByteAddress = new ConcurrentDictionary<int, Dictionary<int, List<int>>>();                       
            int segmentThread = userPreference["system"].maxSegmentThread;  
            int segment = Convert.ToInt32(Math.Round((double)(csvLength / segmentThread), 0));

            abc123Segment.Add(0);
            int nextChar = -1;

            for (int i = 1; i < segmentThread; i++)
            {
                do
                {
                    nextChar++;
                } while (abc123[segment * i + nextChar] != nextRow[nextRow.Count - 1]);

                abc123Segment.Add(segment * i + nextChar + 1);
            }
            if (segment * segmentThread < csvLength)
                abc123Segment.Add(csvLength);

            
            ConcurrentDictionary<int, ImportCSV> concurrentSegmentAddress = new ConcurrentDictionary<int, ImportCSV>();

            for (int worker = 0; worker < abc123Segment.Count - 1; worker++) concurrentSegmentAddress.TryAdd(worker, new ImportCSV());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = userPreference["system"].maxSegmentThread
            };

            Parallel.For(0, abc123Segment.Count - 1, options, currentSegment =>
            {
                tempByteAddress[currentSegment] = concurrentSegmentAddress[currentSegment].findAddress(checkSegmentThreadCompleted, currentSegment, abc123Segment, useDoubleQuote, abc123, maxColumn, nextRow, nextRowChar, separator, csvReadSeparator, openCloseDoubleQuote);
            });
           
           
            do
            {
                Thread.Sleep(10);

            } while (checkSegmentThreadCompleted.Count < abc123Segment.Count - 1);

            for (int currentSegment = 0; currentSegment < abc123Segment.Count - 1; currentSegment++)
                for (int i = 0; i <= maxColumn; i++)
                    byteAddress[i].AddRange(tempByteAddress[currentSegment][i]);          
        }
        public Dictionary<int, List<int>> findAddress(ConcurrentQueue<int> checkSegmentThreadCompleted, int currentSegment, List<int> abc123Segment, bool useDoubleQuote, byte[] abc123, int maxColumn, List<int> nextRow, int nextRowChar, byte separator, byte csvReadSeparator, int openCloseDoubleQuote)
        {
            Dictionary<int, List<int>> tempByteAddress = new Dictionary<int, List<int>>();
            int column = 1;

            for (int i = 0; i <= maxColumn; i++)
                tempByteAddress.Add(i, new List<int>());

            if(currentSegment == 0)            
                tempByteAddress[0].Add(0);          

            var fromAddress = abc123Segment[currentSegment];
            var toAddress = abc123Segment[currentSegment + 1];           

            if (useDoubleQuote == false)
            {
                for (int i = fromAddress; i < toAddress; i++)
                {
                    if (column >= maxColumn)
                    {
                        if (abc123[i] == nextRow[0])
                        {
                            tempByteAddress[maxColumn].Add(i + nextRowChar);
                            tempByteAddress[0].Add(i + nextRowChar);
                            column = 1;
                        }
                        else if (abc123[i] == separator)
                        {
                            useDoubleQuote = true; // unmatch column
                            for (int x = 0; x <= maxColumn; x++)
                                tempByteAddress[x].Clear();
                           
                            if (currentSegment == 0)
                                tempByteAddress[0].Add(0);
                            break;
                        }
                    }

                    if (abc123[i] == separator && column < maxColumn)
                    {
                        tempByteAddress[column].Add(i + 1);
                        column++;
                    }
                }
            }

            if (useDoubleQuote == true) // suspect existence of double quote to hide ","
            {
                separator = csvReadSeparator;
                column = 1;
                openCloseDoubleQuote = 1;
                for (int i = fromAddress; i < toAddress; i++)
                {  
                    if (abc123[i] == 34) // space char
                    {
                        openCloseDoubleQuote = openCloseDoubleQuote * -1;
                        abc123[i] = 32; // replace double quote by space
                    }

                    if (openCloseDoubleQuote == 1)
                        separator = csvReadSeparator;
                    else
                        separator = 127; // del key

                    if (column >= maxColumn)
                    {
                        if (abc123[i] == nextRow[0])
                        {
                            tempByteAddress[maxColumn].Add(i + nextRowChar);
                            tempByteAddress[0].Add(i + nextRowChar);
                            column = 1;
                        }
                    }

                    if (abc123[i] == separator && column < maxColumn)
                    {  
                        tempByteAddress[column].Add(i + 1);                        
                        column++;
                    }
                }
            }            
            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return tempByteAddress;
        }
        public void writeOneColumn(ConcurrentQueue<int> checkThreadCompleted, byte[] abc123, int x, int maxColumn, Dictionary<int, List<int>> byteAddress, ConcurrentDictionary<int, List<double>> ramDetail, ConcurrentDictionary<int, Dictionary<double, string>> remK2V, ConcurrentDictionary<int, Dictionary<string, double>> ramV2K, ConcurrentDictionary<int, bool> isNumType)
        {
            StringBuilder cellValue = new StringBuilder();            
            bool isNumber = true;
            bool tempIsNum = true;
            int byteAddressLength = 100;
            List<double> tryWriteRamDetail = new List<double>();
            List<bool> isWriteNumberSuccess = new List<bool>();
            isWriteNumberSuccess.Add(true);

            if (byteAddress[x].Count < byteAddressLength)
                byteAddressLength = byteAddress[x].Count - 1;            

            for (int y = 1; y < byteAddressLength; y++)
            {
                cellValue.Clear();

                for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)               
                    cellValue.Append((char)abc123[j]);               

                isNumber = double.TryParse(cellValue.ToString().Trim(), out double number);

                if (cellValue.ToString().Trim().Length == 0)
                    isNumber = true;

                tempIsNum = tempIsNum && isNumber;
            }

            if (!isNumType.ContainsKey(x))
                isNumType.TryAdd(x, tempIsNum);

            ImportCSV tryNumber = new ImportCSV();
            tryWriteRamDetail = tryNumber.writeNumberColumn(isWriteNumberSuccess, abc123, x, byteAddress, ramDetail, isNumType);

            if (isNumType[x] == true)
            {
                ramDetail.TryRemove(x, out List<double> value);
                ramDetail.TryAdd(x, tryWriteRamDetail);
            }

            if (isNumType[x] == false) // record string
            {
                if(!ramDetail.ContainsKey(x))
                   ramDetail.TryAdd(x, new List<double>());

                if (x + 1 == maxColumn)
                {
                    for (int y = 1; y < byteAddress[x].Count; y++)
                    {
                        cellValue.Clear();
                        for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)
                        {
                            if (abc123[j] != 13)
                                cellValue.Append((char)abc123[j]);
                        }

                        if (cellValue.ToString().Trim().Length == 0)
                            cellValue.Append("null");

                        string text = cellValue.ToString();

                        if (ramV2K[x].ContainsKey(text)) // same master record
                            ramDetail[x].Add(ramV2K[x][text]);

                        else // add new master record
                        {
                            var count = ramV2K[x].Count;
                            remK2V[x].Add(count, text); // for data 
                            ramV2K[x].Add(text, count);
                            ramDetail[x].Add(count);
                        }
                    }
                }
                else
                {
                    for (int y = 1; y < byteAddress[x].Count; y++)
                    {
                        cellValue.Clear();
                        for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)                        
                                cellValue.Append((char)abc123[j]);                       

                        if (cellValue.ToString().Trim().Length == 0)
                            cellValue.Append("null");

                        string text = cellValue.ToString();

                        if (ramV2K[x].ContainsKey(text)) // same master record
                            ramDetail[x].Add(ramV2K[x][text]);

                        else // add new master record
                        {
                            var count = ramV2K[x].Count;
                            remK2V[x].Add(count, text); // for data 
                            ramV2K[x].Add(text, count);
                            ramDetail[x].Add(count);
                        }
                    }
                }
            }
            checkThreadCompleted.Enqueue(x);           
        }
        public List<double> writeNumberColumn(List<bool> isWriteNumberSuccess, byte[] abc123, int x, Dictionary<int, List<int>> byteAddress, ConcurrentDictionary<int, List<double>> ramDetail, ConcurrentDictionary<int, bool> isNumType)
        {
            List<double> tryWriteRamDetail = new List<double>();

            StringBuilder cellValue = new StringBuilder();
            bool isNumber = true;

            tryWriteRamDetail.Add(0);

            if (isNumType[x] == true) // record number
            {
                //column value
                for (int y = 1; y < byteAddress[x].Count; y++)
                {
                    cellValue.Clear();
                    for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)
                        cellValue.Append((char)abc123[j]);

                    isNumber = double.TryParse(cellValue.ToString().Trim(), out double number);

                    if (isNumber == true)
                    {
                        if (cellValue.ToString().Trim().Length != 0)
                            tryWriteRamDetail.Add(number);
                        else
                            tryWriteRamDetail.Add(0);
                    }
                    else if (cellValue.ToString().Trim().Length == 0)
                    {
                        tryWriteRamDetail.Add(0);
                        isNumber = true;
                    }
                    else if (isNumber == false)
                    {                       
                        isNumType.TryUpdate(x, false, true);
                        isWriteNumberSuccess[0] = false;
                        break;
                    }
                }
            }
            return tryWriteRamDetail;
        }
    }

    public class ExportCSV
    {
        public void ramTable2CSV(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> distinctList, char csvWriteSeparator, string outputFolder, string outputFile)
        {

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);

            for (var cell = 0; cell < distinctList.Count; cell++)
            {
                var e = distinctList[cell][0];
                if (cell > 0) csvString.Append(Separator);
                csvString.Append(distinctList[cell][0]);
            }
            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                for (var cell = 0; cell < distinctList.Count; cell++)
                {
                    if (cell > 0) csvString.Append(Separator);
                    csvString.Append(distinctList[cell][line]);
                }
                csvString.Append(Environment.NewLine);
            }

            using (StreamWriter toDisk = new StreamWriter(outputFolder + userPreference["system"].slash + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void ramDistinct2CSV(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, char csvWriteSeparator, string outputFolder, string outputFile)
        {

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);

            for (var cell = 0; cell < distinctList.Count; cell++)
            {               
                var e = distinctList[cell][0];
                if (cell > 0) csvString.Append(Separator);
                csvString.Append(distinctList[cell][0]);

            }
            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                for (var cell = 0; cell < distinctList.Count; cell++)
                {
                    if (ramKey2Valuegz[cell].Count == 1)
                    {
                        if (cell > 0) csvString.Append(Separator);
                        csvString.Append(distinctList[cell][line].ToString());
                    }
                    if (ramKey2Valuegz[cell].Count > 1)
                    {
                        if (cell > 0) csvString.Append(Separator);
                        csvString.Append(distinctList[cell][line]);
                    }
                }
                csvString.Append(Environment.NewLine);
            }
            using (StreamWriter toDisk = new StreamWriter(outputFolder + userPreference["system"].slash + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void drillDown2CSVymTable(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> ramDetailgz, List<decimal> distinctSet, Dictionary<decimal, List<int>> distinctList2DrillDown, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, char csvWriteSeparator, string outputFolder, string outputFile)
        {
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);
            csvString.Append("Set");
            csvString.Append(Separator);
            for (var cell = 0; cell < ramKey2Valuegz.Count; cell++)
            {
                if (cell > 0) csvString.Append(Separator);

                var currentCell = ramKey2Valuegz[cell][0].ToString().Trim();

                if (currentCell.Contains(Separator))
                    csvString.Append((char)34 + currentCell + (char)34);
                else
                    csvString.Append(currentCell);
            }
            csvString.Append(Environment.NewLine);

            for (var set = 0; set < distinctSet.Count; set++)
            {
                for (var line = 0; line < distinctList2DrillDown[distinctSet[set]].Count; line++)
                {
                    csvString.Append(set + 1);
                    csvString.Append(Separator);
                    for (var cell = 0; cell < ramKey2Valuegz.Count; cell++)
                    {
                        if (ramKey2Valuegz[cell].Count == 1)
                        {
                            if (cell > 0)
                                csvString.Append(Separator);
                                              
                            var currentCell = ramDetailgz[cell][distinctList2DrillDown[distinctSet[set]][line]].ToString().Trim();                                              

                            if (currentCell.Contains(Separator))
                                csvString.Append((char)34 + currentCell + (char)34);
                            else
                                csvString.Append(currentCell);
                        }
                        if (ramKey2Valuegz[cell].Count > 1)
                        {
                            if (cell > 0)
                                csvString.Append(Separator);

                            var currentCell = ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[set]][line]]].ToString().Trim();

                            if (ramKey2Valuegz[cell][0].ToUpper().Contains("DATE"))
                            {

                                isDateNumber = double.TryParse(currentCell, out dateNumber);
                                if (isDateNumber == true)
                                {
                                    if (dateNumber > 1000 && dateNumber < 401770)
                                    {
                                        dateValue = DateTime.FromOADate(dateNumber);
                                        dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                        currentCell = dateFormat;
                                    }
                                }
                            }

                            if (currentCell.Contains(Separator))
                                csvString.Append((char)34 + currentCell + (char)34);
                            else
                                csvString.Append(currentCell);
                        }
                    }
                    csvString.Append(Environment.NewLine);
                }
            }
            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            using (StreamWriter toDisk = new StreamWriter(outputFolder + userPreference["system"].slash + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();

        }
        public void ramDistinct2CSVymTable(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, char csvWriteSeparator, string outputFolder, string outputFile)
        {
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);
            csvString.Append("Set");            
            for (var cell = 0; cell < distinctList.Count; cell++)
            {               
                csvString.Append(Separator);

                var currentCell = distinctRamKey2Value[cell][distinctList[cell][0]].ToString().Trim();

                if (currentCell.Contains(Separator))
                    csvString.Append((char)34 + currentCell + (char)34);
                else
                    csvString.Append(currentCell);
            }
            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                csvString.Append(line);
                csvString.Append(Separator);
                for (var cell = 0; cell < distinctList.Count; cell++)
                {
                    if (distinctRamKey2Value[cell].Count == 1)
                    {
                        if (cell > 0)
                            csvString.Append(Separator);

                        var currentCell = distinctList[cell][line].ToString().Trim();

                        if (currentCell.Contains(Separator))
                            csvString.Append((char)34 + currentCell + (char)34);
                        else
                            csvString.Append(currentCell);
                    }
                    if (distinctRamKey2Value[cell].Count > 1)
                    {
                        if (cell > 0)
                            csvString.Append(Separator);

                        var currentCell = distinctRamKey2Value[cell][distinctList[cell][line]].ToString().Trim(); ;

                        if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(currentCell, out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    currentCell = dateFormat;
                                }
                            }
                        }

                        if (currentCell.Contains(Separator))
                            csvString.Append((char)34 + currentCell + (char)34);
                        else
                            csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }

            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            using (StreamWriter toDisk = new StreamWriter(outputFolder + userPreference["system"].slash + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void ramDistinct2CSVcrosstabTable(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> XdistinctList, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict,  Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, char csvWriteSeparator, string outputFolder, string outputFile)
        {
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);

            int yHeaderCol = 0;
            for (var cell = 0; cell < distinctList.Count; cell++)
            {
                if (distinctList[cell][0] == 0)
                {
                    yHeaderCol++;
                }
                else
                {
                    break;
                }
            }

            int yxmHeaderCol = yHeaderCol + ((XdistinctList[0].Count - 1) * requestDict[requestID].measurement.Count);

            for (int j = 0; j < XdistinctList.Count; j++)
            {
                for (int cell = 0; cell < yHeaderCol; cell++)
                {
                    if (cell > 0) csvString.Append(Separator);
                    csvString.Append(" ");
                }

                for (int cell = 0; cell < XdistinctList[0].Count - 1; cell++)
                {
                    for (var i = 0; i < requestDict[requestID].measurement.Count; i++)
                    {
                        csvString.Append(Separator);

                        var currentCell = distinctXramKey2Value[j][XdistinctList[j][cell + 1]];
                        if (distinctXramKey2Value[j][XdistinctList[j][0]].ToString().ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(currentCell, out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    currentCell = dateValue.ToString("dd.MMM.yy");
                                }
                            }
                        }

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }

            for (int cell = 0; cell < yHeaderCol; cell++)
            {
                if (cell > 0) csvString.Append(Separator);

                var currentCell = distinctRamKey2Value[cell][distinctList[cell][0]];

                if (currentCell.Contains(Separator))
                    csvString.Append("\"" + currentCell + "\"");
                else
                    csvString.Append(currentCell);
            }

            for (var cell = 0; cell < XdistinctList[0].Count - 1; cell++)
            {
                for (var i = 0; i < requestDict[requestID].measurement.Count; i++)
                {
                    csvString.Append(Separator);

                    var currentCell = requestDict[requestID].measurement[i].Replace("#", " ");

                    if (currentCell.Contains(Separator))
                        csvString.Append("\"" + currentCell + "\"");
                    else
                        csvString.Append(currentCell);
                }
            }

            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                for (var cell = 0; cell < yxmHeaderCol; cell++)
                {
                    if (cell < yHeaderCol && distinctRamKey2Value[cell].Count == 1)
                    {
                        if (cell > 0) csvString.Append(Separator);

                        var currentCell = distinctList[cell][line].ToString();

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);

                    }
                    if (cell >= yHeaderCol)
                    {
                        if (cell > 0) csvString.Append(Separator);

                        var currentCell = distinctList[cell][line].ToString();

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }

                    if (cell < yHeaderCol && distinctRamKey2Value[cell].Count > 1)
                    {
                        if (cell > 0) csvString.Append(Separator);

                        var currentCell = distinctRamKey2Value[cell][distinctList[cell][line]];

                        if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(currentCell, out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    currentCell = dateFormat;
                                }
                            }
                        }

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }
            using (StreamWriter toDisk = new StreamWriter(outputFolder + userPreference["system"].slash + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void dimensionMasterData2CSV(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, char csvWriteSeparator, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, string outputFolder)
        {
            StringBuilder master = new StringBuilder(); // export master record
            string Separator = Convert.ToString(csvWriteSeparator);

            for (int i = 0; i < remK2V[requestDict[requestID].importFile].Count; i++)
            {
                foreach (var member in remK2V[requestDict[requestID].importFile][i])
                {
                    if (remK2V[requestDict[requestID].importFile][i][0] != member.Value)
                    if(member.Value.Contains(Separator))
                          master.AppendLine(requestDict[requestID].importFile + "," + remK2V[requestDict[requestID].importFile][i][0] + "," + (char)34 + member.Value.ToString().Trim() + (char)34);
                    else
                          master.AppendLine(requestDict[requestID].importFile + "," + remK2V[requestDict[requestID].importFile][i][0] + "," + member.Value);
                }
            }

            if (!Directory.Exists(outputFolder + userPreference["system"].slash + "dimensionMasterData" + userPreference["system"].slash))
                Directory.CreateDirectory(outputFolder + userPreference["system"].slash + "dimensionMasterData" + userPreference["system"].slash);

            using (StreamWriter toDisk = new StreamWriter(outputFolder + userPreference["system"].slash + "dimensionMasterData" + userPreference["system"].slash + requestDict[requestID].importFile))
            {
                toDisk.Write(master);
                toDisk.Close();
            }
        }
    }
}
