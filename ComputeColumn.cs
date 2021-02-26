using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace youFast
{
    class ComputeColumn
    {
        public void mapColumn(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V, string outputFolder, char csvWriteSeparator, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, List<double>> _ramDetailgz, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz)
        {
            Dictionary<int, List<double>> ramDetailgz = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> ramKey2Valuegz = new Dictionary<int, Dictionary<double, string>>();

            for (int i = 0; i < _ramKey2Valuegz.Count; i++)
            {
                ramDetailgz.Add(i, _ramDetailgz[i]);
                ramKey2Valuegz.Add(i, _ramKey2Valuegz[i]);
            }
         
            string joinKey;
            Dictionary<int, string> sourceColumnID2MappingFileName = new Dictionary<int, string>();
            responseDict[requestID].isMappingKeyFound = false;
            foreach (var file in ramK2V)
            {
                for (int i = 0; i < ramK2V[file.Key].Count; i++)
                {
                    if (ramK2V[file.Key][i][0].ToUpper().Contains("@KEY"))
                    {
                        responseDict[requestID].isMappingKeyFound = true;

                        joinKey = ramK2V[file.Key][i][0].ToUpper().Substring(0, ramK2V[file.Key][i][0].Length - 4);

                        for (int j = 0; j < _ramKey2Valuegz.Count; j++)
                        {
                            if (_ramKey2Valuegz[j][0].ToUpper() == joinKey)
                            {
                                sourceColumnID2MappingFileName.Add(j, file.Key); // link data source column to target table name                               
                                
                            }
                        }
                    }
                }
            }

            if(sourceColumnID2MappingFileName.Count == 0)
                responseDict[requestID].isMappingKeyFound = false;

            if (sourceColumnID2MappingFileName.Count > 0)
            {

                if (requestDict[requestID].debugOutput == "Y")
                {
                    foreach (var pair in sourceColumnID2MappingFileName)
                    {
                        Console.WriteLine("sourceColumnID2MappingFileName " + pair.Key + "  " + pair.Value);

                        foreach (var mapping in ramMapping[pair.Value])
                        {
                            Console.WriteLine("mapping " + mapping.Key + "  " + mapping.Value);

                            foreach (var mappingRow in mapping.Value)
                            {
                                Console.WriteLine("mappingRow " + mappingRow.Key + "  " + mappingRow.Value);
                            }
                        }
                    }
                }

                Dictionary<string, Dictionary<string, double>> mapKeyV2K = new Dictionary<string, Dictionary<string, double>>();

                int line = 0; ;
                foreach (var i in sourceColumnID2MappingFileName)
                {
                    mapKeyV2K.Add(i.Value, new Dictionary<string, double>());

                    foreach (var j in ramK2V[i.Value][0])
                    {
                        if (line == 0) // mark matched key e.g. ACCOUNT
                            mapKeyV2K[i.Value].Add(_ramKey2Valuegz[i.Key][0], j.Key);
                        else
                            mapKeyV2K[i.Value].Add(j.Value, j.Key);

                        // Console.WriteLine("zz mapKeyV2K Count " + j.Value + " " + j.Key);
                        line++;
                    }

                    // Console.WriteLine("mapKeyV2K Count " + mapKeyV2K[i.Value].Count);
                }

                Dictionary<string, Dictionary<double, double>> sourceK2MapK = new Dictionary<string, Dictionary<double, double>>();

                foreach (var i in sourceColumnID2MappingFileName)
                {
                    sourceK2MapK.Add(i.Value, new Dictionary<double, double>());

                    foreach (var j in _ramKey2Valuegz[i.Key])
                    {                       
                        sourceK2MapK[i.Value].Add(j.Key, mapKeyV2K[i.Value][j.Value]);
                    }                   
                }

                int g = 0;
                int currentColumn = _ramKey2Valuegz.Count;

                if (responseDict[requestID].isMappingKeyFound == true && responseDict[requestID].isMapping == false)
                {                   
                    _ramDetailgz.Clear();
                    _ramKey2Valuegz.Clear();
                }

                foreach (var i in sourceColumnID2MappingFileName) // i is source column id of ramDetail
                {
                    for (int c = 0; c < (currentColumn + ramMapping[i.Value].Count); c++)
                    {
                        _ramDetailgz.Add(c, new List<double>());
                        _ramKey2Valuegz.Add(c, new Dictionary<double, string>());
                    }

                    do
                    {
                        if (g < i.Key + 1)
                        {
                            _ramDetailgz[g] = ramDetailgz[g];
                            _ramKey2Valuegz[g] = ramKey2Valuegz[g];
                        }

                        if (g >= i.Key + 1 && g <= i.Key + ramMapping[i.Value].Count)
                        {
                            foreach (var k in ramMapping[i.Value])
                            {
                                for (int row = 0; row < _ramDetailgz[i.Key].Count; row++)
                                    _ramDetailgz[g].Add(ramMapping[i.Value][k.Key][sourceK2MapK[i.Value][_ramDetailgz[i.Key][row]]]);

                                _ramKey2Valuegz[g] = ramK2V[i.Value][k.Key];

                                g++;
                            }

                        }

                        if (g > i.Key + ramMapping[i.Value].Count)
                        {
                            _ramDetailgz[g] = ramDetailgz[g - ramMapping[i.Value].Count];
                            _ramKey2Valuegz[g] = ramKey2Valuegz[g - ramMapping[i.Value].Count];
                        }

                        g++;

                    } while (g < ramKey2Valuegz.Count + ramMapping[i.Value].Count);
                }

                if (requestDict[requestID].debugOutput == "Y")
                {
                    foreach (var i in sourceColumnID2MappingFileName)
                    {
                        Console.WriteLine(_ramKey2Valuegz[i.Key][0]);
                        foreach (var k in ramMapping[i.Value])
                        {
                            for (int row = 0; row < _ramDetailgz[i.Key].Count; row++)
                            {
                                Console.WriteLine("zz " + ramMapping[i.Value].Count + " " + k.Key + " " + ramK2V[i.Value][k.Key][ramMapping[i.Value][k.Key][0]] + " " + ramMapping[i.Value][k.Key][sourceK2MapK[i.Value][_ramDetailgz[i.Key][row]]]);                            
                            }
                        }
                    }

                    ExportCSV currentExport = new ExportCSV();
                    currentExport.ramDistinct2CSV(userPreference, _ramDetailgz, _ramKey2Valuegz, csvWriteSeparator, outputFolder + userPreference["system"].slash + "debug", "mapping-numberOnly" + ".csv");
                    currentExport.ramDistinct2CSVymTable(userPreference, _ramDetailgz, _ramKey2Valuegz, csvWriteSeparator, outputFolder, userPreference["system"].slash + "debug" + userPreference["system"].slash + "mapping" + ".csv");
                }
            }
        }
        public (List<double> oneRamDetail, Dictionary<double, string> oneRamK2V) computeFact(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> ramDetail, Dictionary<int, Dictionary<double, string>> ramK2V)
        {
            
            List<double> oneRamDetail = new List<double>();
            Dictionary<double, string> oneRamK2V = new Dictionary<double, string>();            

            oneRamK2V.Add(0, "Fact");
            oneRamDetail.Add(0);

            List<int> multiSegment = new List<int>();
            int segmentThread = userPreference["system"].maxSegmentThread;

            if (ramDetail[0].Count < 1000)
                segmentThread = 1;

            int segment = Convert.ToInt32(Math.Round((double)(ramDetail[0].Count / segmentThread), 0));

            int line = 1;
            int maxLine = ramDetail[0].Count;
            do
            {
                multiSegment.Add(line);
                line = line + segment;

            } while (line < maxLine);
            multiSegment.Add(maxLine);

            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, ComputeColumn> thread = new ConcurrentDictionary<int, ComputeColumn>();
            ConcurrentDictionary<int, List<double>> oneRamDetailSegment = new ConcurrentDictionary<int, List<double>>();

            for (int worker = 0; worker < multiSegment.Count - 1; worker++)
                thread.TryAdd(worker, new ComputeColumn());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = userPreference["system"].maxSegmentThread
            };

            Parallel.For(0, multiSegment.Count - 1, options, currentSegment =>
            {
                oneRamDetailSegment.TryAdd(currentSegment, thread[currentSegment].ComputeFactOneSegment(multiSegment, currentSegment, checkSegmentThreadCompleted));
            });

            do
            {
                Thread.Sleep(1);
            } while (checkSegmentThreadCompleted.Count < multiSegment.Count - 1);

            for (int currentSegment = 0; currentSegment < multiSegment.Count - 1; currentSegment++)
                oneRamDetail.AddRange(oneRamDetailSegment[currentSegment]);

            return (oneRamDetail, oneRamK2V); 
        }       
        public List<double> ComputeFactOneSegment(List<int> multiSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted)
        {
            List<double> oneRamDetail = new List<double>();

            var fromAddress = multiSegment[currentSegment];
            var toAddress = multiSegment[currentSegment + 1];

            for (int i = fromAddress; i < toAddress; i++)
                oneRamDetail.Add(1);

            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return oneRamDetail;
        }
        public (List<double> oneRamDetail, Dictionary<double, string> oneRamK2V1) computePeriod(string AddPeriodName, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, List<double>> ramDetail, Dictionary<int, Dictionary<double, string>> ramK2V)
        {
            List<double> oneRamDetail = new List<double>();
            ConcurrentDictionary<double, string> oneRamK2V = new ConcurrentDictionary<double, string>();
            ConcurrentDictionary<string, double> oneRamV2K = new ConcurrentDictionary<string, double>();
            List<int> firstDateColumn = new List<int>();

            string dateColumn;
            bool isNumber = true;
            bool isPeriodExist = false;
            double maxDate = 0;
            double minDate = 9999999;
            for (int c = 0; c < ramK2V.Count; c++)
            {
                dateColumn = ramK2V[c][0].ToString().ToUpper();
                if (dateColumn.Contains(AddPeriodName.ToString().ToUpper()))
                    isPeriodExist = true;
            }
            for (int c = 0; c < ramK2V.Count; c++)
            {
                dateColumn = ramK2V[c][0].ToString().ToUpper();
                if (dateColumn.Contains("DATE"))
                {
                    for (int i = 1; i < ramK2V[c].Count; i++)
                    {
                        isNumber = double.TryParse(ramK2V[c][i], out double num);
                        if (num > maxDate)
                            maxDate = num;

                        if (num < minDate)
                            minDate = num;

                        if (isNumber == false)
                            break;
                    }
                    if (isNumber == true)
                    {                       
                        oneRamK2V.TryAdd(0, AddPeriodName);
                        oneRamDetail.Add(0);
                        oneRamV2K.TryAdd(AddPeriodName, 0);
                        firstDateColumn.Add(c);
                        break;
                    }
                }
            }           

            if (firstDateColumn.Count > 0 && isPeriodExist == false)
            {                
                var startYear = DateTime.FromOADate(minDate).Year;
                var endYear = DateTime.FromOADate(maxDate).Year;
                string _month;
                int p = 1;
                for (int year = startYear; year <= endYear; year++)
                {
                    for (int month = 1; month <= 12; month++)
                    {
                        if (month < 10)
                            _month = "0" + month.ToString();
                        else
                            _month = month.ToString();

                        var period = year.ToString() + "m" + _month;
                        oneRamK2V.TryAdd(p, period);
                        oneRamV2K.TryAdd(period, p);
                        p++;

                        if (month == 12)
                        {                        
                            period = "!" + year.ToString() + "m" + _month; // for retained account
                            oneRamK2V.TryAdd(p, period);
                            oneRamV2K.TryAdd(period, p);
                            p++;
                        }
                        
                    }
                }

                List<int> multiSegment = new List<int>();
                int segmentThread = userPreference["system"].maxSegmentThread;

                if (ramDetail[0].Count < 1000)
                    segmentThread = 1;

                int segment = Convert.ToInt32(Math.Round((double)(ramDetail[0].Count / segmentThread), 0));
                int line = 1;
                int maxLine = ramDetail[0].Count;
                do
                {
                    multiSegment.Add(line);
                    line = line + segment;

                } while (line < maxLine);
                multiSegment.Add(maxLine);

                ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
                ConcurrentDictionary<int, ComputeColumn> thread = new ConcurrentDictionary<int, ComputeColumn>();
                ConcurrentDictionary<int, List<double>> oneRamDetailSegment = new ConcurrentDictionary<int, List<double>>();

                for (int worker = 0; worker < multiSegment.Count - 1; worker++)
                    thread.TryAdd(worker, new ComputeColumn());

                var options = new ParallelOptions()
                {
                    MaxDegreeOfParallelism = userPreference["system"].maxSegmentThread
                };

                Parallel.For(0, multiSegment.Count - 1, options, currentSegment =>
                {  
                    oneRamDetailSegment.TryAdd(currentSegment, thread[currentSegment].computePeriodChangeOneSegment(multiSegment, currentSegment, checkSegmentThreadCompleted, firstDateColumn, oneRamK2V, oneRamV2K, ramDetail, ramK2V));                  
                });

                do
                {
                    Thread.Sleep(1);
                } while (checkSegmentThreadCompleted.Count < multiSegment.Count - 1);

                for (int currentSegment = 0; currentSegment < multiSegment.Count - 1; currentSegment++)                
                    oneRamDetail.AddRange(oneRamDetailSegment[currentSegment]);
            }
            Dictionary<double, string> oneRamK2V1 = new Dictionary<double, string>(oneRamK2V);           

            return (oneRamDetail, oneRamK2V1);
        }
        public List<double> computePeriodChangeOneSegment(List<int> multiSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<int> firstDateColumn, ConcurrentDictionary<double, string> oneRamK2V, ConcurrentDictionary<string, double> oneRamV2K, Dictionary< int, List<double>> ramDetail, Dictionary<int, Dictionary<double, string>> ramK2V)
        {
            List<double> oneRamDetail = new List<double>();
            double text;
            DateTime date;
            string period;
            string month = null;
            var fromAddress = multiSegment[currentSegment];
            var toAddress = multiSegment[currentSegment + 1];

            for (int i = fromAddress; i < toAddress; i++)
            {
                text = Convert.ToDouble(ramK2V[firstDateColumn[0]][ramDetail[firstDateColumn[0]][i]]);              
                date = DateTime.FromOADate(text);
                if (date.Month < 10)
                    month = "0" + date.Month.ToString();
                else
                    month = date.Month.ToString();

                period = date.Year.ToString() + "m" + month;
               
                if (oneRamV2K.ContainsKey(period))
                    oneRamDetail.Add(oneRamV2K[period]);
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return oneRamDetail;
        }
        public List<double> computePeriodEndOneSegment(List<int> multiSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<int> firstDateColumn, ConcurrentDictionary<double, string> oneRamK2V, ConcurrentDictionary<string, double> oneRamV2K, Dictionary<int, List<double>> ramDetail, Dictionary<int, Dictionary<double, string>> ramK2V)
        {
            List<double> oneRamDetail = new List<double>();
            
            var fromAddress = multiSegment[currentSegment];
            var toAddress = multiSegment[currentSegment + 1];

            var lastMasterRecord = ramDetail[ramDetail.Count - 1].Max();            

            for (int i = fromAddress; i < toAddress; i++)
               oneRamDetail.Add(lastMasterRecord);
            
            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return oneRamDetail;
        }
    }
}
