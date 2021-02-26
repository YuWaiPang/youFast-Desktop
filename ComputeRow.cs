using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace youFast
{
    public class ComputeRow
    {
        public (Dictionary<int, List<double>> ramDetailBalTable, Dictionary<int, Dictionary<double, string>> remKey2ValueBalTable) calcBalance(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, string outputFolder, char csvWriteSeparator, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, List<double>> distinctList)
        {
            //UNDER DEVELOPMENT
            Dictionary<double, string> retainedDimensionCriteria = new Dictionary<double, string>(); // selected code range of particular dimension                       
            //Transfer: Account(6100~8900) to Account(5100)
            string retainedAccountDimension = "Account";
            string retainedAccount = "5200";
            double retainedAccountKey = 0;
            string startPLaccount = "6100";
            string endPLaccount = "8900";

            int accountDimension = -1; // false

            for (int i = 0; i < distinctRamKey2Value.Count; i++)
            {
                if (distinctRamKey2Value[i][0].ToString().ToUpper() == retainedAccountDimension.ToUpper())
                    accountDimension = i;               
            }

            if (distinctRamKey2Value.ContainsKey(accountDimension))
            {                
                if (distinctRamKey2Value[accountDimension].Count > 1) // dimension
                {
                    for (double j = 1; j < distinctRamKey2Value[accountDimension].Count; j++)
                    {
                        if (string.Compare(distinctRamKey2Value[accountDimension][j].ToString().Replace(" ", ""), startPLaccount) >= 0 && string.Compare(distinctRamKey2Value[accountDimension][j].ToString().Replace(" ", ""), endPLaccount) <= 0)
                            retainedDimensionCriteria.Add(j, distinctRamKey2Value[accountDimension][j]); // output criteria dictionary for filtering data
                    }
                }
           
                foreach (var account in distinctRamKey2Value[accountDimension]) 
                    if (account.Value.ToString() == retainedAccount)
                        retainedAccountKey = account.Key;
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                for (int j = 0; j < distinctRamKey2Value[responseDict[requestID].periodChangeColumn].Count; j++)
                    Console.WriteLine("periodChangeColumn " + responseDict[requestID].periodChangeColumn + " periodRow " + distinctRamKey2Value[responseDict[requestID].periodChangeColumn][j]);
            }
          

            Dictionary<double, Dictionary<int, List<double>>> distinctListMovement = new Dictionary<double, Dictionary<int, List<double>>>();
            Dictionary<double, Dictionary<int, List<double>>> distinctListBalance = new Dictionary<double, Dictionary<int, List<double>>>();
            ConcurrentQueue<int> checkThreadCompleted1 = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, ComputeRow> writeMovementThread = new ConcurrentDictionary<int, ComputeRow>();

            int maxPeriod = distinctRamKey2Value[responseDict[requestID].periodChangeColumn].Count;
            int maxColumn = distinctRamKey2Value.Count;

            for (int currentPeriod = 0; currentPeriod < maxPeriod; currentPeriod++)
            {
                distinctListMovement.Add(currentPeriod, new Dictionary<int, List<double>>());
                distinctListBalance.Add(currentPeriod, new Dictionary<int, List<double>>());

                for (int column = 0; column < distinctRamKey2Value.Count; column++)
                {
                    distinctListMovement[currentPeriod].Add(column, new List<double>());
                    distinctListBalance[currentPeriod].Add(column, new List<double>());
                }
            }

            for (int worker = 0; worker < maxColumn; worker++)
                writeMovementThread.TryAdd(worker, new ComputeRow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = userPreference["system"].maxColumnThread
            };

            Parallel.For(0, maxColumn, options, column =>
            {
                writeMovementThread[column].writeMovementOneColumn(column, checkThreadCompleted1, distinctList, distinctListMovement, requestID, responseDict);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted1.Count < maxColumn);

            ConcurrentQueue<int> checkThreadCompleted2 = new ConcurrentQueue<int>();

            if (distinctRamKey2Value.ContainsKey(accountDimension))
            {
                ComputeRow accountCol = new ComputeRow();
                accountCol.writePreBalTableOneColumn(accountDimension, checkThreadCompleted2, maxPeriod, distinctListMovement, distinctListBalance, distinctRamKey2Value, requestID, responseDict, retainedAccountDimension, retainedDimensionCriteria, retainedAccount, accountDimension, retainedAccountKey);
            }
           
            ConcurrentDictionary<int, ComputeRow> writeColumnThread = new ConcurrentDictionary<int, ComputeRow>();

            for (int worker = 0; worker < maxColumn; worker++)
                writeColumnThread.TryAdd(worker, new ComputeRow());
           
            Parallel.For(0, maxColumn, options, column =>
            {
                if(column != accountDimension)
                    writeColumnThread[column].writePreBalTableOneColumn(column, checkThreadCompleted2, maxPeriod, distinctListMovement, distinctListBalance, distinctRamKey2Value, requestID, responseDict, retainedAccountDimension, retainedDimensionCriteria, retainedAccount, accountDimension, retainedAccountKey);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted2.Count < maxColumn);

            ConcurrentQueue<int> checkThreadCompleted3 = new ConcurrentQueue<int>();

            Dictionary<int, List<double>> ramDetailBalTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> remKey2ValueBalTable = new Dictionary<int, Dictionary<double, string>>();
            ConcurrentDictionary<int, ComputeRow> writeBalanceTableOneColumnThread = new ConcurrentDictionary<int, ComputeRow>();

            for (int column = 0; column <= maxColumn; column++)
                ramDetailBalTable.Add(column, new List<double>());

            remKey2ValueBalTable.Add(0, new Dictionary<double, string>());
            remKey2ValueBalTable[0].Add(0, "Period End");

            for (int i = 1; i < distinctRamKey2Value[responseDict[requestID].periodChangeColumn].Count; i++)
                remKey2ValueBalTable[0].Add(i, distinctRamKey2Value[responseDict[requestID].periodChangeColumn][i]);

            for (int i = 1; i <= distinctRamKey2Value.Count; i++)
                remKey2ValueBalTable.Add(i, distinctRamKey2Value[i - 1]);

            for (int i = 1; i < remKey2ValueBalTable.Count; i++)
            {
                if (remKey2ValueBalTable[i][0] == "Period End")               
                    remKey2ValueBalTable[i][0] = "Period Change";                
            }

            for (int i = 0; i < remKey2ValueBalTable.Count; i++)
                ramDetailBalTable[i].Add(0);

            for (int worker = 0; worker <= maxColumn; worker++)
                writeBalanceTableOneColumnThread.TryAdd(worker, new ComputeRow());

            maxColumn = maxColumn + 1;

            Parallel.For(0, maxColumn, options, column =>
            {
                writeBalanceTableOneColumnThread[column].writeBalTableOneColumn(column, checkThreadCompleted3, maxPeriod, distinctListBalance, ramDetailBalTable);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted3.Count < maxColumn);

            if (requestDict[requestID].debugOutput == "Y")
                Console.WriteLine("ramDetailBalTable.Count " + ramDetailBalTable.Count + " " + "ramDetailBalTable[0].Count " + ramDetailBalTable[0].Count + " " + ramDetailBalTable[1].Count + " " + "remKey2ValueBalTable.Count " + remKey2ValueBalTable.Count + " " + remKey2ValueBalTable[0].Count + " " + remKey2ValueBalTable[0].Count);
           
            int sumMovement = 0;
            foreach (var pair in distinctListMovement)
            {
                for (int j = 0; j < distinctListMovement[0].Count; j++)
                {
                    if (j == 0)
                        sumMovement = sumMovement + distinctListMovement[pair.Key][j].Count;
                }
            }

            int sumBalance = 0;
            foreach (var pair in distinctListBalance)
            {
                for (int j = 0; j < distinctListBalance[0].Count; j++)
                {
                    if (j == 0)
                        sumBalance = sumBalance + distinctListBalance[pair.Key][j].Count;
                }
            }
           
            if (requestDict[requestID].debugOutput == "Y")
                Console.WriteLine("sumMovement " + sumMovement + " " + "sumBalance " + sumBalance);

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "debug"))
                    Directory.CreateDirectory(outputFolder + "debug");

                ExportCSV currentExport = new ExportCSV();
                currentExport.ramDistinct2CSV(userPreference, ramDetailBalTable, remKey2ValueBalTable, csvWriteSeparator, outputFolder + userPreference["system"].slash + "debug", "BalanceTable-numberOnly" + ".csv");
                currentExport.ramDistinct2CSVymTable(userPreference, ramDetailBalTable, remKey2ValueBalTable, csvWriteSeparator, outputFolder, userPreference["system"].slash + "debug" + userPreference["system"].slash + "BalanceTable" + ".csv");
            }
            return (ramDetailBalTable, remKey2ValueBalTable);
        }

        public void writeMovementOneColumn(int column, ConcurrentQueue<int> checkThreadCompleted1, Dictionary<int, List<double>> distinctList, Dictionary<double, Dictionary<int, List<double>>> distinctListMovement, decimal requestID, ConcurrentDictionary<decimal, clientMachine.response> responseDict)
        {
            for (int line = 1; line< distinctList[0].Count; line++)              
                    distinctListMovement[distinctList[responseDict[requestID].periodChangeColumn][line]][column].Add(distinctList[column][line]);

            checkThreadCompleted1.Enqueue(column);
        }

        public void writePreBalTableOneColumn(int column, ConcurrentQueue<int> checkThreadCompleted2, int maxPeriod, Dictionary<double, Dictionary<int, List<double>>> distinctListMovement, Dictionary<double, Dictionary<int, List<double>>> distinctListBalance, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, decimal requestID, ConcurrentDictionary<decimal, clientMachine.response> responseDict, string retainedAccountDimension, Dictionary<double, string> retainedDimensionCriteria, string retainedAccount, int accountDimension, double retainedAccountKey)
        {
            for (int currentPeriod = 0; currentPeriod < maxPeriod; currentPeriod++)
            {
                if (currentPeriod == 0)
                        distinctListBalance[currentPeriod][column].AddRange(distinctListMovement[currentPeriod][column]);

                if (currentPeriod > 0)
                {
                    distinctListBalance[currentPeriod][column].AddRange(distinctListBalance[currentPeriod - 1][column]);
                    distinctListBalance[currentPeriod][column].AddRange(distinctListMovement[currentPeriod][column]);                      
                }

                if (distinctRamKey2Value.ContainsKey(accountDimension))
                {
                    if (distinctRamKey2Value[responseDict[requestID].periodChangeColumn][currentPeriod].ToString().Contains("!")) // reverse P/L balance at ! period
                    {
                        var totalLine = distinctListBalance[currentPeriod][column].Count;

                        for (int i = 0; i < totalLine; i++)
                        {
                            if (retainedDimensionCriteria.ContainsKey(distinctListBalance[currentPeriod][accountDimension][i]))
                                distinctListBalance[currentPeriod][column][i] = retainedAccountKey;
                        }
                    }
                }
            }
            checkThreadCompleted2.Enqueue(column);           
        }

        public Dictionary<int, List<double>> writeBalTableOneColumn(int column, ConcurrentQueue<int> checkThreadCompleted3, int maxPeriod, Dictionary<double, Dictionary<int, List<double>>> distinctListBalance, Dictionary<int, List<double>> ramDetailBalTable)
        {            
            if (column == 0)
            {
                for (int currentPeriod = 0; currentPeriod < maxPeriod; currentPeriod++)                
                    for (int line = 0; line < distinctListBalance[currentPeriod][0].Count; line++)
                        ramDetailBalTable[0].Add(currentPeriod);
            }
            else if (column > 0)
            {
                for (int currentPeriod = 0; currentPeriod < maxPeriod; currentPeriod++)    
                    ramDetailBalTable[column].AddRange(distinctListBalance[currentPeriod][column - 1]);               
            }
            checkThreadCompleted3.Enqueue(column);
            return ramDetailBalTable;
        }
    }
}
