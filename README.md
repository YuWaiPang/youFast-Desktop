# youFast-Desktop
# youFast-Desktop
This youFast version is designed for running a calculation engine and a websocket server in Windows Desktop. 

It supports Windows 7 and Windows 10.

youFast Desktop has following dependency:-

Microsoft .net 4.7.2 or 4.8  
OR Microsoft .net core 3.1 or above

http server is using bluehost.com with url http://desktop.youfast.net/

Fleck Websocket Library:  https://github.com/statianzo/Fleck

Please copy the folder of "Fleck/src/Fleck/" to your project folder along with other .cs files.

CSS Framework: w3.css (built css code within C# code)

Javascript Framework / Library: none

If the download file icon you see in your computer is the same as the icon in StartupScreen.png, you success to run youFast Desktop, otherwise it is running youFast Cloud.

You can download youFast Desktop runtime from https://www.linkedin.com/pulse/youfast-yu-wai-pang-max/

After you click the runtime file, a folder "uSpace" will be created automatically.Inside the folder you can see there are 32 csv files with folder "exportedFile". This folder is used to keep all downloaded report files. Please note that all simulated file names are reserved by the system, so your data file should not use these file names.

Currently youFast Cloud is hosted in Oracle Linux Cloud VM at http://cloud.youfast.net using Oracle Cloud Free Tier https://www.oracle.com/hk/cloud/free/

An article titled "youFast.net is going to the Cloud" https://www.linkedin.com/pulse/youfast-net-going-azure-cloud-yu-wai-pang-max/

email: max@youfast.net

Website: youfast.net

Company Name: YOUFAST.NET (優快通)

Sponsor Site: https://www.patreon.com/lamiyu

Linkedin profile: https://www.linkedin.com/in/max01/

youTube: https://www.youtube.com/channel/UCQoQkyJJBj3k1N9J3Qx1qhA/videos


=============================================================================

Features:- 

➾ using C#, Vanilla JavaScript and W3.css for an integrated frontend to backend Web development, it do not implement any javascript framework or library

➾  real-time web technology use Fleck websocket, source code is embedded in the project

➾  the project do not have dependency other than .net 4.7.2, .net 5 or .net core 3.1, actually the web implementation has not yet using asp.net or asp.net core

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



