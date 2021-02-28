# Crosstab Report Query from Its In-memory DB
![10 Million Rows Test](https://github.com/YuWaiPang/youFast-Desktop/blob/main/youFast-10MillionRowQuery.png)

# How-To
This youFast Desktop edition is designed for running a calculation engine and a websocket server in your Windows Desktop. It also supports to run in Linux, you need to install Visual Studio Code and build the runtime in Linux environment. In fact the youFast Cloud (http://cloud.youFast.net) is hosted in a linux cloud VM using Ubuntu 20.04 using Oracle Cloud Free Tier service (https://www.oracle.com/hk/cloud/free/). If there are a demand in the future, may create one more open source repository to distribute youFast Cloud and how to documentation - enable you to build the youFast Cloud linux runtime and configuration of Oracle Cloud.

It supports Windows 7, Windows 10 and Ubuntu Desktop 18.04 ~ 20.04 

youFast Desktop has the following dependencies:-

➾ Microsoft .net 4.7.2 or 4.8  
➾ OR Microsoft .net core 3.1 or above

➾ Fleck Websocket Library:  https://github.com/statianzo/Fleck

Please copy the folder of "Fleck/src/Fleck/" to your project folder along with other .cs files.

➾ CSS Framework: w3.css (built css code within C# code)

➾ Javascript Framework / Library: none

If you use Visual Studio to create a new project, please be reminded the following settings:-
1) Create folder same as assembly name and namespace "youFast"
2) Add reference: "System.IO.Compression.FileSystem
3) Drag and drop folder "Fleck" and other cs file into C# youFast (one item below "Solution 'youFast' (1 of 1 project))
4) Configure Platform target as x64
5) Remove default project file "Program.cs", Startup.cs has contained static void Main.

See this demo video using Visual Studio 2019 Community Edition to build the runtime of the open source "youFast Desktop''. https://youtu.be/W51MFvmVH38

If you use Visual Studio Code to create a new project, it is obviously simple.

➾ http server is using bluehost.com with url http://desktop.youfast.net/

   Note: dotnet supports auto start this home page and auto open download file, but dotnet core does not support this function.

If the download file icon you see in your computer is the same as the icon in the StartupScreen.png, you success to run youFast Desktop, otherwise it is running youFast Cloud.

You can download youFast Desktop runtime from https://www.linkedin.com/pulse/youfast-yu-wai-pang-max/

After you click the runtime file, a folder "uSpace" will be created automatically.Inside the folder you can see there are 32 csv files with folder "exportedFile". This folder is used to keep all downloaded report files. Please note that all simulated csv file names are reserved by the system, so your data files should not use these file names.

Demo Video: https://www.youtube.com/channel/UCQoQkyJJBj3k1N9J3Qx1qhA/videos

1MillionRow.csv: https://drive.google.com/file/d/1NnFV4F45lpA-3AHkdWrkwioGHLqeCRDV/view?usp=sharing 

10MillionRow.csv: https://drive.google.com/file/d/1qmx5lKCni4qGNXfRyNjhK8UpBuTisYwM/view?usp=sharing 

# youFast Desktop for Linux

If you build the runtime for Ubuntu Desktop using dotnetcore 3.1, please amend 2 values of the Startup.cs.
The contributor has tested for Ubuntu Linux and Oracle Linux, you can try other Linux.

➾ userPreference["system"].os = "Linux"; 

➾ userPreference["system"].slash = "/";  

# Features

➾  using C#, Vanilla JavaScript and W3.css for an integrated frontend to backend Web development, it does not implement any javascript framework or library

➾  real-time web technology use Fleck websocket

➾  the project does not have dependency other than .net 4.7.2, .net 5 or .net core 3.1, actually the web implementation has not yet using asp.net or asp.net core

➾  build-in a key-value NoSQL database for in-memory and disk versions

➾  building a set of algorithms to maximize parallel computing operating units which is extended to a big csv file and an in-memory table

➾  ultra-fast data import to web crosstab report with drag & drop and drill down capabilities

➾  interactive pivot table that lets you move X and Y columns with real-time drilldown by simple mouse actions

➾  crosstab report supports multi-level of analysis account trial balance by period/currency/region

➾  1.3+ second youFast can process a million rows of a csv file to produce a web summary report and 0.13+ second youFast can filter data from 10 million rows to produce a web crosstab report (testing machine: Dell OptiPlex 7070 Micro Form Factor with Intel Core i9-9900 8 Cores 32G Ram Windows 10)

➾  developing pagination in X and Y direction, different levels of numeric precision

➾  clicking an app file with only 500KB, user can enjoy zero installation and implementation

➾  the app can run on a share drive and USB memory stick

➾  this project is maintained up to 100% C# source code (i.e. no call for external dll)

# youFast.net Contacts

youFast is developing in China (Hong Kong) - a wholly work from home project by a programmer.

Email: max@youfast.net

Website: youfast.net

Company Name: YOUFAST.NET (優快通)

Sponsor Site: https://www.patreon.com/lamiyu

Linkedin Profile: https://www.linkedin.com/in/max01/

An article titled "youFast.net is going to the Cloud" https://www.linkedin.com/pulse/youfast-net-going-azure-cloud-yu-wai-pang-max/

# Sundry Matter

youFast does not follow traditional approach to build data cubes to support multi-dimensional reporting. It is because building a data cube is a very heavily computing and time consuming exercise, so it is unlikely this approach can support real-time reporting. So youFast has built its distinctive algorithm to achieve real-time multidimensional reporting. 

If your SQL Server implements the youFast algorithm, your SQL Server multidimensional reporting may be significantly faster.

The contributor now evaluates the next open source project. Since he has a strong background in the finance department of some listed companies, his preference will consider to build a set of reusable front-end and back-end libraries to support software developers building their next generation of Finance ERP.
