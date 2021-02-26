using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fleck;

namespace youFast
{
    public class WebSockAgentServer
    {      
        Dictionary<string, Dictionary<int, List<double>>> ramDetail = new Dictionary<string, Dictionary<int, List<double>>>();
        Dictionary<string, Dictionary<int, Dictionary<double, string>>> ramK2V = new Dictionary<string, Dictionary<int, Dictionary<double, string>>>();
        Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping = new Dictionary<string, Dictionary<int, Dictionary<double, double>>>();
        ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact = new ConcurrentDictionary<string, ConcurrentDictionary<int, int>>();        
        Dictionary<int, StringBuilder> htmlTable = new Dictionary<int, StringBuilder>();
        ConcurrentDictionary<decimal, int> cancelRequestID = new ConcurrentDictionary<decimal, int>(); // list of completed request  
        ConcurrentDictionary<decimal, Thread> queueThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage queue job
        ConcurrentQueue<decimal> incomingRequestQueue = new ConcurrentQueue<decimal>(); // current o/s queue job
        ConcurrentDictionary<decimal, string> incomingRequest = new ConcurrentDictionary<decimal, string>(); // keep system generate requestID and message
        ConcurrentDictionary<decimal, Thread> request2Response = new ConcurrentDictionary<decimal, Thread>(); // each thread manage each report to pagination
        Dictionary<int, Dictionary<string, string>> processAcceptList = new Dictionary<int, Dictionary<string, string>>();
        Dictionary<int, bool> resetDimensionOrder = new Dictionary<int, bool>();
        Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl = new Dictionary<decimal, Dictionary<string, StringBuilder>>();
        ConcurrentDictionary<int, decimal> currentRequestID = new ConcurrentDictionary<int, decimal>();
        ConcurrentDictionary<string, int> copyMemDetailRandomly = new ConcurrentDictionary<string, int>();
        ConcurrentDictionary<decimal, string> requestID2SessionIDsending = new ConcurrentDictionary<decimal, string>();
        ConcurrentDictionary<decimal, string> requestID2SessionIDsent = new ConcurrentDictionary<decimal, string>();
        List<IWebSocketConnection> wsServerSessionID = new List<IWebSocketConnection>();
        ConcurrentDictionary<decimal, IWebSocketConnection> requestID2SessionID = new ConcurrentDictionary<decimal, IWebSocketConnection>();
        ConcurrentDictionary<string, bool> isResponse = new ConcurrentDictionary<string, bool>();

        public async void webSock(string errorMessageLog, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, int iteration, string outputFolder, byte csvReadSeparator, char csvWriteSeparator, Dictionary<int, string> forwardMessage, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, string sourceFolder, string db1Folder, string dbBackupFolder)
        {   
            resetDimensionOrder[0] = false;
            processAcceptList.Add(1, new Dictionary<string, string>());
            processAcceptList.Add(2, new Dictionary<string, string>());        
            processAcceptList[2].Add("dataSorting", "interimEvent");
            processAcceptList[2].Add("moveToCrosstab", "interimEvent");
            processAcceptList[2].Add("removeFromCrosstab", "interimEvent");
            processAcceptList[2].Add("rotateDimension", "interimEvent");
            processAcceptList[2].Add("rotateDimensionCrosstab", "interimEvent");
            processAcceptList[2].Add("rotateDimensionDrillDown", "interimEvent");
            processAcceptList[2].Add("displayPrecision", "finalEvent");
            processAcceptList[2].Add("nextPrecision", "finalEvent");
            processAcceptList[2].Add("changeDrillDownEvent", "finalEvent");            
            processAcceptList[2].Add("drillDown", "finalEvent");
            processAcceptList[2].Add("pageMove", "finalEvent");
            processAcceptList[2].Add("downloadReport", "finalEvent");
            processAcceptList[2].Add("openReportByWindows", "finalEvent");

            int serialID = 0; // to force uniqueness of html ID for adding filter control
            bool isRemove = false;  
            DateTime currentDateTime = DateTime.Now;
            Random random = new Random();  

            string myHost = System.Net.Dns.GetHostName();
            string myIP = null;
            int IP4v = 0;
            Dictionary<int, string> myIP4V_List = new Dictionary<int, string>();

            if (userPreference["system"].os == "Linux")
            {
                foreach (var netInterface in NetworkInterface.GetAllNetworkInterfaces())
                {
                    if (netInterface.NetworkInterfaceType == NetworkInterfaceType.Wireless80211 ||
                        netInterface.NetworkInterfaceType == NetworkInterfaceType.Ethernet)
                    {
                        foreach (var addrInfo in netInterface.GetIPProperties().UnicastAddresses)
                        {
                            if (addrInfo.Address.AddressFamily == AddressFamily.InterNetwork)
                            {
                                IP4v++;
                                var ipAddress = addrInfo.Address;                                
                                myIP4V_List.Add(IP4v, ipAddress.ToString());
                            }
                        }
                    }
                }
            }


            if (userPreference["system"].os == "Windows Client" || userPreference["system"].os == "Windows Server")
            {
                for (int i = 0; i < System.Net.Dns.GetHostEntry(myHost).AddressList.Length; i++)
                {
                    if (System.Net.Dns.GetHostEntry(myHost).AddressList[i].IsIPv6LinkLocal == false)
                    {
                        IP4v++;
                        myIP = System.Net.Dns.GetHostEntry(myHost).AddressList[i].ToString();
                        myIP4V_List.Add(IP4v, myIP);
                    }
                }
            }
           
            var countIP = myIP4V_List.Count;

            if (userPreference["system"].os == "Windows Client")
            {               
                userPreference["system"].serverIP = "127.0.0.1";
            }
            else if (countIP > 1)
            {
                foreach (KeyValuePair<int, string> item in myIP4V_List)
                {
                    Console.WriteLine("Option {0} IP:{1}", item.Key, item.Value);
                }
                Console.Write("Please select your option: ");
                var select = Console.ReadLine();
                int selected = Int32.Parse(select);
                userPreference["system"].serverIP = myIP4V_List[selected];
            }
            else
            {
                userPreference["system"].serverIP = myIP4V_List[1];
            }

            Console.WriteLine("youFast websocket server is starting at " + userPreference["system"].serverIP + ":" + userPreference["system"].websocketPort);                     
           
            StringBuilder html = new StringBuilder();          

            byte[] contentByte = Encoding.ASCII.GetBytes(html.ToString());

            
            string htmlHeader = "HTTP/1.0 200 OK" + "\r\n" +
                "Server: WebServer 1.0" + "\r\n" +
                "Content-Length: " + contentByte.Length + "\r\n" +
                "Content-Type: text/html" +
                "\r\n" + "\r\n";
            

            byte[] headerByte = Encoding.ASCII.GetBytes(htmlHeader);         
            

            if (userPreference["system"].os == "Windows Client")
                System.Diagnostics.Process.Start("http://desktop.youfast.net");

            WebSocketServer wsServer = new WebSocketServer("ws://" + userPreference["system"].serverIP + ":" + userPreference["system"].websocketPort);
           
            queueThread.TryAdd(1, new Thread(() => requestQueue2Thread(requestID2SessionIDsending, requestID2SessionIDsent, isResponse, requestID2SessionID, wsServerSessionID, errorMessageLog, ramMapping, copyMemDetailRandomly, userPreference, tableFact, currentRequestID, sourceFolder, csvReadSeparator, db1Folder, iteration, outputFolder, csvWriteSeparator, clientSessionVariable, requestDict,  responseDict, forwardMessage, htmlTable, ramDetail, ramK2V, incomingRequestQueue, request2Response, cancelRequestID, isRemove, currentDateTime, screenControl)));
            queueThread[1].Start();        

            decimal userRequestCount = 0;
            
            wsServer.Start(socket =>
            {               
                socket.OnOpen = () =>
                {
                    wsServerSessionID.Add(socket);                
                 
                    Console.WriteLine(string.Format("youFast websocket client " + socket.ConnectionInfo.Id + " connected at " + socket.ConnectionInfo.ClientIpAddress + ":" + socket.ConnectionInfo.ClientPort + " Current connecton count is " + wsServerSessionID.Count));                                       
                   
                };

                socket.OnClose = () =>
                {
                    wsServerSessionID.Remove(socket);
                };
             
                socket.OnMessage = async message =>
                {
                    forwardMessage[0] = message;

                  //  Console.WriteLine(message + " ");             

                    if (message.Contains("{") == true) // message may be JSON                
                    {
                        decimal requestID = 0;
                        DateTime dt = DateTime.Now;
                        userRequestCount++;
                        requestID = (userRequestCount * 1000 + dt.Millisecond) / 1000;

                        if (!responseDict.ContainsKey(requestID))                        
                            responseDict.TryAdd(requestID, new clientMachine.response());

                        if (!requestID2SessionID.ContainsKey(requestID))
                            requestID2SessionID.TryAdd(requestID, socket);                       

                        responseDict[requestID].serverSession = socket.ConnectionInfo.Id.ToString();
                        responseDict[requestID].startTime = DateTime.Now;

                        if (responseDict[requestID].serverSession != null)                      
                            clientSessionVariable.TryAdd(responseDict[requestID].serverSession, new clientMachine.clientSession()); // new a clientSession   

                        if (!isResponse.ContainsKey(responseDict[requestID].serverSession))                       
                            isResponse.TryAdd(responseDict[requestID].serverSession, true);                      

                        if (isResponse[responseDict[requestID].serverSession] == true)
                        {
                            Json convert = new Json();

                            await Task.Run(() => convert.Json2VariableArray(message));
                            await Task.Run(() => convert.Json2VariableList(message, requestID, requestDict));

                            if (processAcceptList[2].ContainsKey(requestDict[requestID].processID) == true)
                            {
                                isResponse[responseDict[requestID].serverSession] = false;
                                await Task.Run(() => convert.Json2VariableList(message, 2, requestDict));
                            }

                            else if (requestDict[requestID].processID == "css") // next process is "function displayFilterDropDownList()", where processID = "selectFile" 
                            {
                                CSS currentExport = new CSS();
                                await Task.Run(() => currentExport.css(isResponse, requestID2SessionID, wsServerSessionID, requestID, responseDict));
                            }

                            else if (requestDict[requestID].processID == "addFilter")
                            {                             
                                serialID++;                                
                                ExportHTML currentExport = new ExportHTML();
                                await Task.Run(() => currentExport.ramdistinct2AddFilter(isResponse, requestID2SessionID, wsServerSessionID, userPreference, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, serialID, ramK2V[requestDict[requestID].importFile]));
                            }

                            else if (requestDict[requestID].processID == "addDisplayColumn")
                            {                                
                                ExportHTML currentExport = new ExportHTML();
                                serialID++;                                
                                if (requestDict[requestID].addColumnType == "AddAllDistinctDimension")
                                    await Task.Run(() => currentExport.ramdistint2DisplayAllColumn(isResponse, requestID2SessionID, wsServerSessionID, userPreference, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, ramK2V[requestDict[requestID].importFile]));

                                if (requestDict[requestID].addColumnType == "AddCrosstabDimension")
                                    await Task.Run(() => currentExport.ramdistint2Crosstab(isResponse, requestID2SessionID, wsServerSessionID, userPreference, tableFact, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, ramK2V[requestDict[requestID].importFile]));

                                if (requestDict[requestID].addColumnType == "AddAllMeasurement")
                                    await Task.Run(() => currentExport.ramdistint2AllMeasurement(isResponse, requestID2SessionID, wsServerSessionID, userPreference, outputFolder, requestID, clientSessionVariable, requestDict, responseDict, ramK2V[requestDict[requestID].importFile]));
                            }

                            else if (requestDict[requestID].processID == "importSelectedFile" || requestDict[requestID].processID == "runReport")
                            {
                                isResponse[responseDict[requestID].serverSession] = false;
                                currentRequestID[0] = requestID;
                                requestDict[requestID].processID = "";
                                if (incomingRequest.TryAdd(requestID, message) == true)
                                    incomingRequestQueue.Enqueue(requestID);
                            }
                        }
                    }                
                };

                socket.OnBinary = message =>
                {

                };
               
        });
          
        }
     
        public void requestQueue2Thread(ConcurrentDictionary<decimal, string> requestID2SessionIDsending, ConcurrentDictionary<decimal, string> requestID2SessionIDsent, ConcurrentDictionary<string, bool> isResponse, ConcurrentDictionary<decimal, IWebSocketConnection> requestID2SessionID, List<IWebSocketConnection> wsServerSessionID, string errorMessageLog, Dictionary<string, Dictionary<int, Dictionary<double, double>>> ramMapping, ConcurrentDictionary<string, int> copyMemDetailRandomly, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<int, decimal> currentRequestID, string sourceFolder, byte csvReadSeparator, string db1Folder, int iteration, string outputFolder, char csvWriteSeparator, ConcurrentDictionary<string, clientMachine.clientSession> clientSessionVariable, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, string> forwardMessage, Dictionary<int, StringBuilder> htmlTable, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, ConcurrentQueue<decimal> incomingRequestQueue, ConcurrentDictionary<decimal, Thread> request2Response, ConcurrentDictionary<decimal, int> cancelRequestID, bool isRemove, DateTime currentDateTime, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl)
        {
            Request2Report processRequest = new Request2Report();

            while (true)
            {
                if (incomingRequestQueue.Count > 0)
                {
                    decimal requestID;
                    if (!incomingRequestQueue.TryPeek(out requestID))
                        Console.WriteLine("TryPeek failed when it should have succeeded");

                    else if (requestID != 0)
                    {
                        if (!incomingRequestQueue.TryDequeue(out requestID))
                            Console.WriteLine("TryDeqeue failed when it should have succeeded");

                        else if (requestID != 0)
                        {
                            try
                            {
                                if (request2Response.TryAdd(requestID, new Thread(() => processRequest.distinctDBreporting(isResponse, requestID2SessionID, wsServerSessionID, errorMessageLog, ramMapping, copyMemDetailRandomly, userPreference, tableFact, currentRequestID, csvWriteSeparator, responseDict, htmlTable, ramDetail, remK2V, sourceFolder, outputFolder, csvReadSeparator, db1Folder, requestID, clientSessionVariable, requestDict, screenControl))) == true)
                                    request2Response[requestID].Start();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"request2Response fail '{e}'");
                            }
                        }
                    }                   
                }
                Thread.Sleep(2);
            }
        }
    }
}
