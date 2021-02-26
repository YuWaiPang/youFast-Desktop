using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;

namespace youFast
{
    class SelectionCriteria
    {
        public (Dictionary<int, List<string>> selectionCriteria, List<int> distinctDimension, List<int> crosstabDimension) validateSelectionCrieteria(decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<string, string> columnName2ID, Dictionary<int, List<double>> ramDetailgz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            Dictionary<int, List<string>> selectionCriteria = new Dictionary<int, List<string>>();
            List<int> distinctDimension = new List<int>(); // for muti-dimensional distinct dimension                            
            List<int> crosstabDimension = new List<int>(); // for crosstab dimension (included in distinct dimension)                     

            selectionCriteria.Add(0, new List<string>()); // column
            selectionCriteria.Add(1, new List<string>()); // startOption
            selectionCriteria.Add(2, new List<string>()); // startColumnValue
            selectionCriteria.Add(3, new List<string>()); // endOption
            selectionCriteria.Add(4, new List<string>()); // endColumnValue
            selectionCriteria.Add(5, new List<string>()); // distinctDimension
            selectionCriteria.Add(6, new List<string>()); // crosstabDimension
            selectionCriteria.Add(7, new List<string>()); // measurement                                  

            for (int i = 0; i < requestDict[requestID].column.Count; i++)
            { 
                selectionCriteria[0].Add(columnName2ID[requestDict[requestID].column[i].Replace("#", " ")]); //convert column name to ID
                selectionCriteria[1].Add(requestDict[requestID].startOption[i]);

                var startColumn = requestDict[requestID].startColumnValue[i].Replace(";", ":");
                startColumn = startColumn.Replace("|", "/");
                selectionCriteria[2].Add(startColumn);

                selectionCriteria[3].Add(requestDict[requestID].endOption[i]);

                var endColumn = requestDict[requestID].endColumnValue[i].Replace(";", ":");
                endColumn = endColumn.Replace("|", "/");
                selectionCriteria[4].Add(endColumn);
            }

            // selection criteria = filter (0 ~ 4) + distinct dimension (5) + crosstab dimension (6)
            for (int i = 0; i < requestDict[requestID].distinctDimension.Count; i++)
                selectionCriteria[5].Add(columnName2ID[requestDict[requestID].distinctDimension[i].Replace("#", " ")]); // add filter column ID

            // selectionCriteria[6] => crosstab dimension (string)
            if (requestDict[requestID].crosstabDimension != null)
            {
                for (int i = 0; i < requestDict[requestID].crosstabDimension.Count; i++)
                    selectionCriteria[6].Add(columnName2ID[requestDict[requestID].crosstabDimension[i].Replace("#", " ")]);
            }

            List<Double> validateLogElementCount = new List<double>();
            List<Double> validateFactor = new List<double>();

            // distinct dimension (number)
            for (int i = 0; i < selectionCriteria[5].Count; i++)
            {
                bool success = Int32.TryParse(selectionCriteria[5][i], out int column);
                distinctDimension.Add(column);
            }

            // crosstab dimension (number)
            for (int i = 0; i < selectionCriteria[6].Count; i++)
            {
                bool success = Int32.TryParse(selectionCriteria[6][i], out int column);
                crosstabDimension.Add(column);
                if (!distinctDimension.Contains(column))
                {
                    distinctDimension.Add(column);
                    selectionCriteria[5].Add(column.ToString());
                }
            }

            decimal validateDistinctDimensionChecksum = 0;

            for (int i = distinctDimension.Count - 1; i > 0; i--)
            {
                if (i == distinctDimension.Count - 1) validateLogElementCount.Add(Math.Round((Math.Log(ramKey2Valuegz[distinctDimension[i]].Count, 10) + 0.5000001), 0));
                if (i < distinctDimension.Count - 1) validateLogElementCount.Add(Math.Round((Math.Log(ramKey2Valuegz[distinctDimension[i]].Count, 10) + 0.5000001), 0) + validateLogElementCount[distinctDimension.Count - 2 - i]);
            }

            for (int i = 0; i < (distinctDimension.Count - 1); i++)
                validateFactor.Add(Math.Pow(10, validateLogElementCount[i]));

            if (validateFactor.Count > 0) // > 1 column
            {
                int dd = 0;
                do
                {
                    if (dd < distinctDimension.Count - 1)
                    {
                        if (Math.Ceiling(Math.Log(Convert.ToDouble(validateDistinctDimensionChecksum) + Convert.ToDouble(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1] * validateFactor[distinctDimension.Count - 2 - dd]), 10)) < 29)
                            validateDistinctDimensionChecksum = validateDistinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1] * validateFactor[distinctDimension.Count - 2 - dd]);
                        else
                        {
                            removeOneDistinctDimension();
                            removeOneDistinctDimension();
                            removeOneDistinctDimension();
                            validateDistinctDimensionChecksum = 0;
                            dd = -1;
                        }
                    }

                    if (dd == distinctDimension.Count - 1)
                    {
                        if (Math.Ceiling(Math.Log(Convert.ToDouble(validateDistinctDimensionChecksum) + Convert.ToDouble(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1]), 10)) < 29)
                        {
                            validateDistinctDimensionChecksum = validateDistinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1]);
                        }
                        else
                        {
                            removeOneDistinctDimension();
                            removeOneDistinctDimension();
                            removeOneDistinctDimension();
                            validateDistinctDimensionChecksum = 0;
                            dd = -1;
                        }
                    }
                    dd++;
                } while (dd <= distinctDimension.Count);

                void removeOneDistinctDimension()
                {
                    var last = distinctDimension.Count - 1;

                    if (crosstabDimension.Contains(distinctDimension[last]))
                    {
                        var lastCrossTab = crosstabDimension.Count - 1;
                        crosstabDimension.RemoveAt(lastCrossTab);
                        var sc6last = selectionCriteria[6].Count - 1;
                        selectionCriteria[6].RemoveAt(sc6last);
                    }
                    if (!crosstabDimension.Contains(distinctDimension[last]))
                    {
                        var sc5last = selectionCriteria[5].Count - 1;
                        selectionCriteria[5].RemoveAt(sc5last);
                    }
                    distinctDimension.RemoveAt(last);
                }
            }
            return (selectionCriteria, distinctDimension, crosstabDimension);
        }
        public Dictionary<int, Dictionary<double, string>> conditional2ExactMatch(Dictionary<int, List<double>> ramDetailgz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<int, List<string>> selectionCriteria)
        {
            Dictionary<int, Dictionary<double, string>> dimensionCriteria = new Dictionary<int, Dictionary<double, string>>(); // selected code range of particular dimension            

            // Convert from conditional match label to exact match 
            for (int i = 0; i < ramDetailgz.Count; i++)
                dimensionCriteria.Add(i, new Dictionary<double, string>());

            for (int i = 0; i < selectionCriteria[0].Count; i++)
            {
                bool success = Int32.TryParse(selectionCriteria[0][i], out int filterColumn); // convert web input string to number                                  

                if (ramKey2Valuegz[filterColumn].Count > 1) // dimension
                {
                    for (double j = 1; j < ramKey2Valuegz[filterColumn].Count; j++)
                    {
                        if (selectionCriteria[1][i].ToString().Replace(" ", "") == "=" && selectionCriteria[3][i].ToString() == "=")
                            if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) == 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) == 0) // =0 means =="L60"
                                conditional2exact();

                        if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">" && selectionCriteria[3][i].ToString() == "<")
                            if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) > 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) < 0)
                                conditional2exact();

                        if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">=" && selectionCriteria[3][i].ToString() == "<=")
                            if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[2][i]) >= 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) <= 0)
                                conditional2exact();

                        if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">" && selectionCriteria[3][i].ToString() == "<=")
                            if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) > 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) <= 0)
                                conditional2exact();

                        if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">=" && selectionCriteria[3][i].ToString() == "<")
                            if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) >= 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) < 0)
                                dimensionCriteria[filterColumn].Add(j, ramKey2Valuegz[filterColumn][j]); // output criteria dictionary for filtering data                   

                        void conditional2exact()
                        {
                            dimensionCriteria[filterColumn].Add(j, ramKey2Valuegz[filterColumn][j]); // output criteria dictionary for filtering data
                        }
                    }
                }
            }
            return dimensionCriteria;
        }
        public (List<int> yDimension, Dictionary<int, string> xyDimension, List<int> revisedX, List<int> revisedY) sortSelectedDimensionOrder(Dictionary<int, List<string>> selectionCriteria, List<int> distinctDimension, decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, StringBuilder debug, List<int> crosstabDimension)
        {
            List<int> yDimension = new List<int>(); // distinct dimension excluding crosstab dimension
            Dictionary<int, string> xyDimension = new Dictionary<int, string>();
            List<int> revisedX = new List<int>(); // excluding other non-distinct dimensions 
            List<int> revisedY = new List<int>(); // excluding other non-distinct dimensions             

            distinctDimension.Sort();

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append(Environment.NewLine);
                debug.Append("Sorted distinctDimension: ");
                foreach (int d in distinctDimension) debug.Append(d + " ");
                debug.Append(Environment.NewLine); debug.Append(Environment.NewLine);
            }

            // y dimension (number)
            for (int i = 0; i < selectionCriteria[5].Count; i++)
            {
                bool success = Int32.TryParse(selectionCriteria[5][i], out int column);
                if (!crosstabDimension.Contains(column))
                    yDimension.Add(column);
            }

            // xy dimension (number) 
            for (int i = 0; i < crosstabDimension.Count; i++)
                xyDimension.Add(crosstabDimension[i], "x");

            for (int i = 0; i < yDimension.Count; i++)
                xyDimension.Add(yDimension[i], "y");

            int empty = 0;
            for (int i = 0; i < xyDimension.Count + empty; i++)
            {
                if (!xyDimension.ContainsKey(i))
                    empty++;

                if (xyDimension.ContainsKey(i))
                {
                    if (xyDimension[i] == "x")
                        revisedX.Add((i - empty));

                    if (xyDimension[i] == "y")
                        revisedY.Add((i - empty));
                }
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append("crosstabDimension: ");
                for (int i = 0; i < crosstabDimension.Count; i++)
                    debug.Append(crosstabDimension[i] + " ");

                debug.Append(" yDimension: ");
                for (int i = 0; i < yDimension.Count; i++)
                    debug.Append(yDimension[i] + " ");

                debug.Append(" revisedX ");
                for (int i = 0; i < revisedX.Count; i++)
                    debug.Append(revisedX[i] + " ");

                debug.Append(" revisedY: ");
                for (int i = 0; i < revisedY.Count; i++)
                    debug.Append(revisedY[i] + " ");

                debug.Append(Environment.NewLine);
            }
            return (yDimension, xyDimension, revisedX, revisedY);
        }
        public (Dictionary<int, List<int>> measurementColumn, Dictionary<int, List<string>> measurementOperator, Dictionary<int, List<double>> measurementRange, List<int> measure, bool noMultipleMeasurementColumn) recordMeasurementSelectionCriteria(decimal requestID, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, List<string>> selectionCriteria, List<int> crosstabDimension, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<string, string> columnName2ID)
        {
              // measure
            Dictionary<int, List<int>> measurementColumn = new Dictionary<int, List<int>>();
            Dictionary<int, List<string>> measurementOperator = new Dictionary<int, List<string>>();
            Dictionary<int, List<double>> measurementRange = new Dictionary<int, List<double>>();
            List<int> measure = new List<int>(); // numerical dimensions excluding Date, Code
            Dictionary<int, int> multipleMeasurementColumn = new Dictionary<int, int>();
            bool noMultipleMeasurementColumn = true;

            measurementColumn.Add(0, new List<int>());
            measurementOperator.Add(0, new List<string>());
            measurementOperator.Add(1, new List<string>());
            measurementRange.Add(0, new List<double>());
            measurementRange.Add(1, new List<double>());
            measurementRange.Add(2, new List<double>());

            for (int i = 0; i < requestDict[requestID].column.Count; i++)
            {
                bool successCol = Int32.TryParse(selectionCriteria[0][i], out int measurementCol); // convert web input string to number                                                                 

                if (ramKey2Valuegz[measurementCol].Count == 1) // if a column has column name only (no master record), assume it is a numerical column
                {
                    measurementColumn[0].Add(measurementCol); //convert column name to ID                       

                    if (multipleMeasurementColumn.ContainsKey(measurementCol))  // if multiple, will affect filter number route
                        noMultipleMeasurementColumn = noMultipleMeasurementColumn && false;

                    if (!multipleMeasurementColumn.ContainsKey(measurementCol))
                        multipleMeasurementColumn.Add(measurementCol, measurementCol);


                    measurementOperator[0].Add(requestDict[requestID].startOption[i]); // add data from requestDict object directly
                    measurementOperator[1].Add(requestDict[requestID].endOption[i]); // add data from requestDict object directly

                    bool successStart = Int32.TryParse(selectionCriteria[2][i], out int startNum); // convert web input string to number                                  
                    bool successEnd = Int32.TryParse(selectionCriteria[4][i], out int endNum); // convert web input string to number                                  

                    measurementRange[0].Add(startNum);
                    measurementRange[1].Add(endNum);
                }
            }

            List<string> addMeasurement = new List<string>();            

            if (crosstabDimension.Count == 0)
                if (requestDict[requestID].measurement == null)
                    requestDict[requestID].measurement = addMeasurement;

            if (crosstabDimension.Count > 0)
                if (requestDict[requestID].measurement == null)
                    requestDict[requestID].measurement = addMeasurement;

            for (int i = 0; i < requestDict[requestID].measurement.Count; i++)
            {
                if (columnName2ID.ContainsKey(requestDict[requestID].measurement[i].Replace("#", " ")))
                    selectionCriteria[7].Add(columnName2ID[requestDict[requestID].measurement[i].Replace("#", " ")]);
            }

            for (int i = 0; i < selectionCriteria[7].Count; i++)
            {
                bool success = Int32.TryParse(selectionCriteria[7][i], out int column);
                measure.Add(column);
            }
            return (measurementColumn, measurementOperator, measurementRange, measure, noMultipleMeasurementColumn);
        }

    }
}
