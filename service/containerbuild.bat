xcopy ..\server\bin\Debug\netcoreapp3.0 .\binary  /s /e /Y

docker build -t pennerz/pine_rtable:v0.7 .\containers\\ReliableTableService\

docker network create -d nat --subnet=172.16.238.0/24 --gateway=172.16.238.1 pinertable_network

docker create -it --name pinertable_node1 --network pinertable_network --ip 172.16.238.10 -v C:\Users\pennerz\source\repos\PineRSL\service\binary:c:\Users\ContainerUser\pinetableservice\ -v C:\Users\pennerz\source\repos\PineRSL\service\config\node1:c:\config pennerz/pine_rtable:v0.7

docker create -it --name pinertable_node2 --network pinertable_network --ip 172.16.238.11 -v C:\Users\pennerz\source\repos\PineRSL\service\binary:c:\Users\ContainerUser\pinetableservice\ -v C:\Users\pennerz\source\repos\PineRSL\service\config\node2:c:\config pennerz/pine_rtable:v0.7

docker create -it --name pinertable_node3 --network pinertable_network --ip 172.16.238.12 -v C:\Users\pennerz\source\repos\PineRSL\service\binary:c:\Users\ContainerUser\pinetableservice\ -v C:\Users\pennerz\source\repos\PineRSL\service\config\node3:c:\config pennerz/pine_rtable:v0.7

docker create -it --name pinertable_node4 --network pinertable_network --ip 172.16.238.13 -v C:\Users\pennerz\source\repos\PineRSL\service\binary:c:\Users\ContainerUser\pinetableservice\ -v C:\Users\pennerz\source\repos\PineRSL\service\config\node4:c:\config pennerz/pine_rtable:v0.7

docker create -it --name pinertable_node5 --network pinertable_network --ip 172.16.238.14 -v C:\Users\pennerz\source\repos\PineRSL\service\binary:c:\Users\ContainerUser\pinetableservice\ -v C:\Users\pennerz\source\repos\PineRSL\service\config\node5:c:\config pennerz/pine_rtable:v0.7



