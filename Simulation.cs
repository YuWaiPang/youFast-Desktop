using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFast
{
    public class Simulation
    {
        public void randomCopyTableMultiThread(ConcurrentDictionary<string, int> copyMemDetailRandomly, int totalColumn, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, decimal requestID)
        {
            ConcurrentDictionary<int, Dictionary<int, List<double>>> randRamDetail = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            ConcurrentQueue<int> checkRandSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, Thread> currentRandomSegment = new ConcurrentDictionary<int, Thread>(); // a thread manage queue job  
            Random random = new Random();
            totalColumn = remK2V[requestDict[requestID].importFile].Count;
            int multiplyRow3;
            multiplyRow3 = random.Next(1, 8);

            Parallel.For(0, multiplyRow3, currentSegment =>
            {
                try // new a thread to manage queue job
                {
                    Simulation addRandData = new Simulation();
                    randRamDetail.TryAdd(currentSegment, new Dictionary<int, List<double>>());
                    currentRandomSegment.TryAdd(currentSegment, new Thread(() => randRamDetail[currentSegment] = addRandData.randomCopyTable(checkRandSegmentThreadCompleted, currentSegment, currentRandomSegment, copyMemDetailRandomly, totalColumn, ramDetail, remK2V, clientSessionVariable, requestDict,  requestID)));
                    currentRandomSegment[currentSegment].Start();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Thread fail '{e}'");
                }
            });

            int processSegment = 0;
            int returnSegment;
            do
            {
                Thread.Sleep(2);

                if (checkRandSegmentThreadCompleted.Count > 0)
                {
                    checkRandSegmentThreadCompleted.TryPeek(out returnSegment);
                    if (randRamDetail.ContainsKey(returnSegment))
                    {
                        if (randRamDetail[returnSegment].ContainsKey(totalColumn - 1))
                        {   
                            checkRandSegmentThreadCompleted.TryDequeue(out returnSegment);

                            for (int j = 0; j < totalColumn; j++)
                                ramDetail[requestDict[requestID].importFile][j].AddRange(randRamDetail[returnSegment][j]);

                            processSegment++;
                        }
                    }
                }

            } while (processSegment < (multiplyRow3 - 1));          
        }
        public Dictionary<int, List<double>> randomCopyTable(ConcurrentQueue<int> checkRandSegmentThreadCompleted, int currentSegment, ConcurrentDictionary<int, Thread> currentRandomSegment, ConcurrentDictionary<string, int> copyMemDetailRandomly, int totalColumn, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, decimal requestID)
        {
            Dictionary<int, List<double>> randRamDetail = new Dictionary<int, List<double>>();
            Random random = new Random();

            for (int i = 0; i < totalColumn; i++)
                randRamDetail.Add(i, new List<double>());

            int randColumn = 0;
            do
            {
                randColumn = random.Next(0, totalColumn);
            } while (remK2V[requestDict[requestID].importFile][randColumn].Count < 2);

            int totalMasterRecord = remK2V[requestDict[requestID].importFile][randColumn].Count;

            Dictionary<double, int> randByMasterRecord = new Dictionary<double, int>();
            randByMasterRecord.Clear();
            for (int i = 1; i < totalMasterRecord; i++)
                randByMasterRecord.Add(i, random.Next(1, 5));

            int multiplyRow1;
            int multiplyRow2;

            multiplyRow1 = random.Next(1, 10);
            var currentNoOfLine = ramDetail[requestDict[requestID].importFile][0].Count;

            for (int i = 1; i < currentNoOfLine; i++)
            {
                multiplyRow2 = random.Next(1, randByMasterRecord[ramDetail[requestDict[requestID].importFile][randColumn][i]]);
                for (int j = 0; j < totalColumn; j++)
                    for (int k = 0; k < (multiplyRow1 * multiplyRow2); k++)
                        randRamDetail[j].Add(ramDetail[requestDict[requestID].importFile][j][i]);
            }

            checkRandSegmentThreadCompleted.Enqueue(currentSegment);
            return randRamDetail;
        }
        public void dimensionValueList(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, int> copyMemDetailRandomly, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, decimal requestID, string sourceFolder, string outputFolder)
        {
            ConcurrentDictionary<string, Thread> simulationThread = new ConcurrentDictionary<string, Thread>();
            List<string> multiDimensionModel = new List<string>();
            
            Dictionary<string, string> calc = new Dictionary<string, string>();
            Dictionary<string, Random> manyRandom = new Dictionary<string, Random>();
            Random random = new Random();
            int numberOfRow;
            int numberOfMater;
            calc.Add("+", "Add");
            calc.Add("-", "Substract");
            calc.Add("*", "Multiply");
            calc.Add("/", "Divide");
            calc.Add("%", "Remainder");
            calc.Add("^", "Power");

            for (int i = 1; i < remK2V[requestDict[requestID].importFile][0].Count; i++)
                multiDimensionModel.Add(remK2V[requestDict[requestID].importFile][0][i]);

            Dictionary<int, string> randomModelPool = new Dictionary<int, string>();
            int m = 0;
            foreach (string currentModel in multiDimensionModel)
            {
                randomModelPool.Add(m, currentModel);
                m++;
            }
                    
            var maxModel = multiDimensionModel.Count;

            if (userPreference["system"].maxStartupSampleTableSimulation > multiDimensionModel.Count)
                userPreference["system"].maxStartupSampleTableSimulation = multiDimensionModel.Count;

            multiDimensionModel.Clear();
            do
            {
                m = random.Next(0, maxModel);
                if (!manyRandom.ContainsKey(randomModelPool[m]))
                { 
                    manyRandom.Add(randomModelPool[m], new Random());
                    multiDimensionModel.Add(randomModelPool[m]);
                }

            } while (manyRandom.Count < userPreference["system"].maxStartupSampleTableSimulation); 

            foreach (string currentModel in multiDimensionModel)
            {
                try 
                {                   
                    numberOfRow = random.Next(1, 10000);                                      
                    numberOfMater = random.Next(1000, 2000);
                   
                    simulationThread.TryAdd(currentModel, new Thread(() => simulateOneTopic(userPreference, copyMemDetailRandomly, manyRandom, numberOfMater, numberOfRow, calc, currentModel, multiDimensionModel, ramDetail, remK2V, clientSessionVariable, requestDict,  requestID, sourceFolder, outputFolder)));
                    simulationThread[currentModel].Start();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Thread fail '{e}'");
                }
            }
        }
        public void simulateOneTopic(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, int> copyMemDetailRandomly, Dictionary<string, Random> manyRandom, int numberOfMater, int numberOfRow, Dictionary<string, string> calc, string currentModel, List<string> multiDimensionModel, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, decimal requestID, string sourceFolder, string outputFolder)
        {
            Dictionary<string, List<string>> masterRecordForEachDimension = new Dictionary<string, List<string>>();
            StringBuilder simaluatedData = new StringBuilder();            
            Dictionary<string, string> dimension2Group = new Dictionary<string, string>();
            Dictionary<string, string> group2Dimension = new Dictionary<string, string>();
            Dictionary<string, List<double>> numberDimensionValue = new Dictionary<string, List<double>>();           
            Dictionary<string, bool> isNumberType = new Dictionary<string, bool>();            
            Dictionary<string, char[]> equation = new Dictionary<string, char[]>();
            StringBuilder recogniseFormula = new StringBuilder();
            Dictionary<string, Dictionary<int, List<string>>> independentVariable = new Dictionary<string, Dictionary<int, List<string>>>();
            List<int> hideOutputDimension = new List<int>();
            List<string> hideOutputColumn = new List<string>();

            int startRandom = 1;
            int endRandom = 1;
            int decimalPlace = 0;
            string startText = "";
            string endText = "";           

            for (int i = 0; i < ramDetail[requestDict[requestID].importFile][0].Count; i++) // build master record by each dimension
            {
                if (remK2V[requestDict[requestID].importFile][0][ramDetail[requestDict[requestID].importFile][0][i]] == currentModel) // Topic
                {
                    var currentDimension = remK2V[requestDict[requestID].importFile][1][ramDetail[requestDict[requestID].importFile][1][i]]; // Dimension
                    var currentGroup = remK2V[requestDict[requestID].importFile][2][ramDetail[requestDict[requestID].importFile][2][i]]; // Group
                    var currentValue1 = remK2V[requestDict[requestID].importFile][3][ramDetail[requestDict[requestID].importFile][3][i]]; // Value1
                    var randomAddValue = manyRandom[currentModel].Next(1, 50);

                    if (currentGroup.Contains("="))
                    {
                        var index = currentGroup.IndexOf('=', 1);
                        var newDimension = currentGroup.Substring(0, index);

                        if (newDimension.Substring(0, 1) == "!")
                        {
                            newDimension = currentGroup.Substring(1, (newDimension.Length - 1));                            
                            if (!hideOutputColumn.Contains(newDimension))
                                hideOutputColumn.Add(newDimension);
                        }

                        var newValue = currentGroup.Substring((index + 1), (currentGroup.Length - index - 1));
                        bool success1 = double.TryParse(newValue, out double newNumber);

                        if (!isNumberType.ContainsKey(newDimension))
                            isNumberType.Add(newDimension, true);
                        else
                            isNumberType[newDimension] = isNumberType[newDimension] && success1;

                        if (!masterRecordForEachDimension.ContainsKey(newDimension))
                        {
                            dimension2Group.Add(currentDimension, newDimension);
                            group2Dimension.Add(newDimension, currentDimension);
                            masterRecordForEachDimension.Add(newDimension, new List<string>());
                            masterRecordForEachDimension[newDimension].Add(newValue.ToString());
                        }
                        else
                            masterRecordForEachDimension[newDimension].Add(newValue.ToString());
                    }


                    if (currentDimension.Substring(0, 1) == "!")
                    {
                        currentDimension = currentDimension.Substring(1, (currentDimension.Length - 1));                       
                        if (!hideOutputColumn.Contains(currentDimension))
                            hideOutputColumn.Add(currentDimension);
                    }


                    if (currentValue1.Contains("~")) // is random number range
                    {
                        if (!isNumberType.ContainsKey(currentDimension))
                            isNumberType.Add(currentDimension, true);

                        var index = currentValue1.IndexOf('~', 1);
                        startText = currentValue1.Substring(0, index);

                        if (startText.IndexOf(".", 0) < 0)
                            decimalPlace = 0;
                        else
                            decimalPlace = startText.Length - startText.IndexOf(".", 0) - 1;

                        endText = currentValue1.Substring((index + 1), (currentValue1.Length - index - 1));

                        if (endText.IndexOf(".", 0) > 0)
                            if (endText.Length - endText.IndexOf(".", 0) - 1 > decimalPlace)
                                decimalPlace = endText.Length - endText.IndexOf(".", 0) - 1;
                      
                        bool success2 = double.TryParse(startText, out double startNum);
                        bool success3 = double.TryParse(endText, out double endNum);
                        if(decimalPlace == 0)
                        {
                            startRandom = Convert.ToInt32(startNum);
                            endRandom = Convert.ToInt32(endNum);
                        }
                        if (decimalPlace > 0)
                        { 
                            startRandom = Convert.ToInt32(startNum * (10 ^ decimalPlace));
                            endRandom = Convert.ToInt32(endNum * (10 ^ decimalPlace));
                        }                     

                        for (int x = 0; x < numberOfMater; x++)
                        {                           
                            double currentNumber = manyRandom[currentModel].Next(startRandom, endRandom); // crash

                            if (decimalPlace > 0)
                            { 
                                for (int d = 0; d < decimalPlace; d++)
                                  currentNumber = currentNumber * 0.1;
                            }

                            if (!masterRecordForEachDimension.ContainsKey(currentDimension))
                            {
                                masterRecordForEachDimension.Add(currentDimension, new List<string>());
                                masterRecordForEachDimension[currentDimension].Add(currentNumber.ToString());
                            }
                            else
                                masterRecordForEachDimension[currentDimension].Add(currentNumber.ToString());
                        }
                    }
                    else // is one number
                    {
                        bool success4 = double.TryParse(currentValue1, out double number);

                        if (!isNumberType.ContainsKey(currentDimension))
                            isNumberType.Add(currentDimension, success4);
                        else
                            isNumberType[currentDimension] = isNumberType[currentDimension] && success4;

                        if (!masterRecordForEachDimension.ContainsKey(currentDimension))
                        {
                            masterRecordForEachDimension.Add(currentDimension, new List<string>());
                            masterRecordForEachDimension[currentDimension].Add(currentValue1);
                        }
                        else
                            masterRecordForEachDimension[currentDimension].Add(currentValue1);
                    }                   

                    if (currentValue1.Length >= 5 && currentValue1.Substring(0, 5).ToUpper().ToString() == "CALC(")  // decomposition of equation
                    {
                        equation.Clear();
                        equation.Add(currentDimension, currentValue1.Substring(5, currentValue1.Length - 6).ToCharArray());

                        foreach (var equationElement in equation)  // y = x * z   equationElement.key is y
                        {
                            int a = 0; int e = 0;
                            if (!independentVariable.ContainsKey(equationElement.Key))
                                independentVariable.Add(equationElement.Key, new Dictionary<int, List<string>>()); //List[0] is + - * / List[1] is x, z

                            foreach (var abc in equationElement.Value) // decomposit to =x, *z
                            {
                                a++;

                                if (!calc.ContainsKey(abc.ToString())) // if not + - * /
                                {
                                    recogniseFormula.Append(abc); // record as independent variable x, z

                                    if (a == equationElement.Value.Length) // last char of the last variable
                                    {
                                        if (!independentVariable[equationElement.Key].ContainsKey(e))
                                            independentVariable[equationElement.Key.ToString().Trim()].Add(e, new List<string>());

                                        if(independentVariable[equationElement.Key][e].Count == 0 && e == 0)
                                        {
                                            independentVariable[equationElement.Key][e].Add("=");
                                            independentVariable[equationElement.Key][e].Add(recogniseFormula.ToString().Trim());
                                            recogniseFormula.Clear();
                                        }

                                        if (independentVariable[equationElement.Key][e].Count == 1)
                                        {
                                            independentVariable[equationElement.Key][e].Add(recogniseFormula.ToString().Trim());
                                            recogniseFormula.Clear();
                                        }
                                    }
                                }
                                else // + - * / exist  
                                {
                                    if (!independentVariable[equationElement.Key].ContainsKey(e))
                                        independentVariable[equationElement.Key].Add(e, new List<string>());

                                    if (e == 0)
                                        independentVariable[equationElement.Key][e].Add("=");

                                    independentVariable[equationElement.Key][e].Add(recogniseFormula.ToString().Trim());
                                    recogniseFormula.Clear();

                                    if (!independentVariable[equationElement.Key].ContainsKey(e + 1))
                                        independentVariable[equationElement.Key].Add((e + 1), new List<string>());

                                    independentVariable[equationElement.Key][e + 1].Add(abc.ToString().Trim());
                                    e++;
                                }
                            }
                        }
                    }
                }
            }

            foreach (var pair in isNumberType) // create list to store number for each numerical dimension
            {
                if (pair.Value == true)
                    numberDimensionValue.Add(pair.Key, new List<double>());
            }

            int k = 0;
            foreach (var pair in masterRecordForEachDimension) // save column name 
            {
                if (!hideOutputColumn.Contains(pair.Key))
                {
                    if (k != masterRecordForEachDimension.Count - 1)
                        simaluatedData.Append(pair.Key + ",");
                    else
                        simaluatedData.AppendLine(pair.Key);
                }
                else
                { 
                    hideOutputDimension.Add(k);                  
                }

                k++;
            }           

            int groupDataRow = -1;
            
            using (StreamWriter toDisk = new StreamWriter(sourceFolder + userPreference["system"].slash + currentModel + ".csv"))
            {
                var file = currentModel.ToString() + ".csv";
                copyMemDetailRandomly.TryAdd(file, numberOfRow);

                for (int i = 0; i < numberOfRow; i++) // save column value
                {
                    k = 0;
                    foreach (var currentDimension in masterRecordForEachDimension)
                    {                      
                        var row = manyRandom[currentModel].Next(0, currentDimension.Value.Count); // crash
                        var comma = k == masterRecordForEachDimension.Count - 1 ? Environment.NewLine : ",";

                        if (group2Dimension.ContainsKey(currentDimension.Key)) // detected user defined group
                        {
                            if (isNumberType[currentDimension.Key] == true)
                            {
                                bool success5 = double.TryParse(currentDimension.Value[row], out double newNumber);
                                numberDimensionValue[currentDimension.Key].Add(newNumber);
                            }

                            if (!hideOutputDimension.Contains(k))
                                simaluatedData.Append(currentDimension.Value[row] + comma);

                            groupDataRow = row;
                        }
                        else if (groupDataRow != -1) // related group to its parent
                        {
                            if (!hideOutputDimension.Contains(k))                             
                                simaluatedData.Append(currentDimension.Value[groupDataRow] + comma);

                            groupDataRow = -1;
                            
                        }
                        else // without user define group
                        {
                            if (isNumberType[currentDimension.Key] == true)
                            {
                                bool success6 = double.TryParse(currentDimension.Value[row], out double newNumber);
                                numberDimensionValue[currentDimension.Key].Add(newNumber);
                            }

                            if (!independentVariable.ContainsKey(currentDimension.Key)) // without user define formula
                            { 
                                if (!hideOutputDimension.Contains(k))
                                    simaluatedData.Append(currentDimension.Value[row] + comma);
                            }
                            else  // with user define formula
                            {
                                foreach (var fGroup in independentVariable)
                                {
                                    if (currentDimension.Key == fGroup.Key)
                                    {
                                        var calcValue = numberDimensionValue[independentVariable[fGroup.Key][0][1]][i];

                                        for (int f = 1; f < independentVariable[fGroup.Key].Count; f++)
                                        {
                                            if (independentVariable[fGroup.Key][f][0] == "+") calcValue = calcValue + numberDimensionValue[independentVariable[fGroup.Key][f][1]][i];
                                            if (independentVariable[fGroup.Key][f][0] == "-") calcValue = calcValue - numberDimensionValue[independentVariable[fGroup.Key][f][1]][i];
                                            if (independentVariable[fGroup.Key][f][0] == "*") calcValue = calcValue * numberDimensionValue[independentVariable[fGroup.Key][f][1]][i];
                                            if (independentVariable[fGroup.Key][f][0] == "/") calcValue = calcValue / numberDimensionValue[independentVariable[fGroup.Key][f][1]][i];
                                            if (independentVariable[fGroup.Key][f][0] == "%") calcValue = calcValue % numberDimensionValue[independentVariable[fGroup.Key][f][1]][i];
                                            if (independentVariable[fGroup.Key][f][0] == "^") calcValue = Math.Pow(calcValue,numberDimensionValue[independentVariable[fGroup.Key][f][1]][i]);
                                        }

                                        calcValue = Math.Round((Double)calcValue, 2);

                                        if (!numberDimensionValue.ContainsKey(currentDimension.Key))
                                        {
                                            numberDimensionValue.Add(currentDimension.Key, new List<double>());
                                            numberDimensionValue[currentDimension.Key].Add(calcValue);
                                        }
                                        else                                                                                    
                                            numberDimensionValue[currentDimension.Key].Add(calcValue);

                                        if(!hideOutputDimension.Contains(k))
                                            simaluatedData.Append(calcValue.ToString() + comma);
                                        
                                    }
                                }
                            }
                        }

                        k++;
                    }
                    if (i % 100000 == 0)
                    {                        
                        toDisk.Write(simaluatedData);
                        simaluatedData.Clear();
                    }
                }
                    
                toDisk.Write(simaluatedData);
                toDisk.Close();               
            }
        } 
    }
}