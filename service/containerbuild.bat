xcopy ..\server\bin\Debug\netcoreapp3.0 .\binary  /s /e /Y

docker build -t pennerz/pinehill_rtable:v0.8 .\containers\\ReliableTableService\

docker network create -d nat --subnet=172.16.238.0/24 --gateway=172.16.238.1 pinertable_network

docker create -it --name pinehillrtable_node1 --network pinertable_network --ip 172.16.238.10 -v C:\Users\pennerz\source\repos\PineHillRSL\service\binary:c:\Users\ContainerUser\pinehilltableservice\ -v C:\Users\pennerz\source\repos\PineHillRSL\service\config\node1:c:\config pennerz/pinehill_rtable:v0.8

docker create -it --name pinehillrtable_node2 --network pinertable_network --ip 172.16.238.11 -v C:\Users\pennerz\source\repos\PineHillRSL\service\binary:c:\Users\ContainerUser\pinehilltableservice\ -v C:\Users\pennerz\source\repos\PineHillRSL\service\config\node2:c:\config pennerz/pinehill_rtable:v0.8

docker create -it --name pinehillrtable_node3 --network pinertable_network --ip 172.16.238.12 -v C:\Users\pennerz\source\repos\PineHillRSL\service\binary:c:\Users\ContainerUser\pinehilltableservice\ -v C:\Users\pennerz\source\repos\PineHillRSL\service\config\node3:c:\config pennerz/pinehill_rtable:v0.8

docker create -it --name pinehillrtable_node4 --network pinertable_network --ip 172.16.238.13 -v C:\Users\pennerz\source\repos\PineHillRSL\service\binary:c:\Users\ContainerUser\pinehilltableservice\ -v C:\Users\pennerz\source\repos\PineHillRSL\service\config\node4:c:\config pennerz/pinehill_rtable:v0.8

docker create -it --name pinehillrtable_node5 --network pinertable_network --ip 172.16.238.14 -v C:\Users\pennerz\source\repos\PineHillRSL\service\binary:c:\Users\ContainerUser\pinehilltableservice\ -v C:\Users\pennerz\source\repos\PineHillRSL\service\config\node5:c:\config pennerz/pinehill_rtable:v0.8



