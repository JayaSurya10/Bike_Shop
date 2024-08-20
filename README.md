# Bike_Shop
**I created an end-to-end data engineering project using Microsoft Azure.**

Hello everyone today I gone be creating end to end data engineering project using azure.
This project is all about moving data from on-premises sql server to cloud.

**On premises sql server **
	•	For that i am using This bike shop database in local.
	•	This database has these set of tables
	•	So i gone a move this tables to cloud using azure.
 
**Azure **
	•	Here already i have created set of resources which are all useful for this project.
	•	Storage accounts, Data factory, data bricks, Azure synapse analytics etc
	•	We need data factory to run pipelines 
	•	So i am opening that first 
 
**Data factory **
	•	Launch studio 
	•	Here i had already Create a pip-line
	•	First i am using look up activity
	•	Here, Look up table is used for fetching table names from the on premise sql
	•	Then i am using For each activity 
	•	Inside For each I am using copy data activity 
	•	In the source of the copy data i am selecting the sql database 
	•	Below that instead if table i am selecting query and i am added this query 
	•	This query fetches schema name and table name only
	•	In the sink dataset i am selecting blob storage and selected csv format
	•	Below there is dataset properties 
	•	Here i selected @item(), @item()
	•	Then in settings of For each activity,
	•	I have added this query which takes  the table name as a value.
	•	This will get the Table name from query which i used in the copy data activity 
	•	This will fetch data from on premises sql server to the mentioned storage account.
	•	Connect the lookup activity with for each activity 
 
**Storage account **
	•	This is the storage account i have created 
	•	This has 4 containers 
	•	Bronze, sliver, Gold, and Diamond
	•	I stored the on premises data in the bronze 
	•	After running that pipeline i will able to see this data
	•	There is 2 folders production and sales
	•	So i am merging 2 folders into 1
 
**Data factory **
	•	For that i am using another copy data activity in the same pip line
	•	In source dataset i am selecting sales folder
	•	And selected file path type as wildcard file path, to select all files from sales folder 
	•	In sink i am selecting production dataset which i need to store sales data in it.
	•	Connect the for each activity with copy data activity 
	•	If i run again the pip line i am able to see sales folder files in production folder 
 
**Data bricks**
	•	Select Launch work space
	•	In this project i am using pyspark in the   Data bricks
	•	Firstly unmounted if there any connection made with the storage account 
	•	Then, Connect the storage account with data bricks 
	•	Source has link for storage account 
	•	Mount point used as a root directory of the files in the storage account 
	•	Extra configs has a link for storage account and the secret key of the storage account 
	•	Then i run that
	•	Verify mount with the storage account is successful by running this code
	•	By this code i am able to see what are the folders in the given container 
	•	Here i am able to read the csv file using spark.read.csv( ) and giving a file location and said this file has header 
	•	Here in production.products on the list price column i am able to see values are not whole numbers
	•	And in staff’s column phone numbers are not in Indian format.
	•	So i am doing both the transformations in data bricks using pyspark
	•	First I created empty list with the name ‘table name’
	•	Using for loop and append function i am able to add every file name in the list
	•	Run that code 
	•	Run table name
	•	I am able to see files in the table name 
 
**1 transformation code**
	•	In this code i am gone make price values in proper format on all the tables which has price in it
	•	Firstly i have used for loop for getting every file name from bronze container 
	•	Using nested for loop i am getting columns which has a word price in it
	•	Using column function and round function i am able to convert price in proper whole numbers 
	•	Then i stored that output in separate sliver1 container.
 
**2nd transformation code**
	•	For the 2nd transformation i need to change phone number format
	•	Need to remove round brackets 
	•	i have used for loop for getting every file name from sliver1 container 
	•	Using nested for loop i am getting columns which has a word phone in it
	•	Using column and regexp_replace function i am able to remove that round brackets
	•	Then i stored that output in separate gold container.
	•	Here we can can see gold1 has files that i have saved now
 
**3rd transformation code:**
	•	In the orders table for the columns which are related to date have a time stamp 00:00
	•	For the date columns I don’t want that
	•	So i am removing that time stamps from all the date columns 
	•	i have used for loop for getting every file name from gold1 container 
	•	Using nested for loop i am getting columns which has a word date in it
	•	Using if statement , date format, from_utc_time_stamps functions and timestamps type i am able to get only dates
	•	Then i stored that output in separate diamond container.
	•	I am able to see the files in diamond container with all transformations
 
**Blob storage to lake storage **
	•	Now i am transferring files from blob storage to date lake storage gen 2 using pyspark 
	•	Firstly unmounted if there any connection made with the storage account 
	•	Then mount the data lake storage with data bricks 
	•	First I created empty list with the name ‘table name’
	•	Using for loop and append function i am able to add every file name in the list
	•	Then using for loop i am able to fetch datas from blob storage.
	•	Using nested for loop i am able to send the datas to data lake storage.
	•	These are the transformations i had done in data bricks using pyspark.
 
**Connecting data factory with data bricks**
	•	There is an activity called notebook in data factory 
	•	With the help of notebook i am able to connect my 2 data bricks workspace with the data factory.
	•	Connect the 2 notebooks with the copy data.
	•	Now if i run the pip-line, it automatically done all the activity mentioned here
	•	Now moving the transformation clean date into cloud with the help of azure synapses analytics.
 
**Azure Synapses Analytics**
	•	Azure synapses analytics is built above on a data factory 
	•	So i am able to do all the activities which i have done in both data factory and data bricks
	•	In data tab first i need to create a database 
	•	Here i already created Diamond_DB Database
	•	In this database only i am gone to store files which i transformed in data bricks.
	•	I have created a stored procedure which has contains openrowset function
	•	In that openrowset function i am able to give link for data lake storage account 
	•	And give the file format i want.
	•	Here i chosen delta format
	•	Use diamond db for the  stored procedure 
	•	Then create a pipeline by selecting Integrate
	•	Drag and drop Get meta data activity 
	•	This is used to get table names 
	•	Create a new binary dataset in data lake storage, which is used to store table names
	•	 Select field list argument as child items
	•	Drag and drop For each activity 
	•	In settings > items > add dynamic content and select getsliver_tables
	•	Then within for each activity, add stored procedure activity which
	•	In settings add new sql database data set and add domain name and database name.
	•	Then Select stored procedure name.
	•	Add stored procedure parameter 
	•	Then if i run the pipline, i am able to fetch data from data lake storage and store them as table format in the views
 
**Power BI**
	•	Open Power BI.
	•	Select Get Data.
	•	Select Azure Synapse Analytics SQL
	•	Click Connect 
	•	Enter the Serverless SQL endpoint code which is in the settings > properties of synapses workspace 
	•	And enter the cloud database name which i created in synapses workspace 
	•	Select Data connectivity mode as import 
	•	Click ok 
	•	Here i need to Select the tables which I want to use for creating reports 
	•	Using different types of visualisation i am able to get this reports of the bike shop 
	•	If any updates made in the on premise database table, it will reflect in this report when i run the pipline
	•	We can run the pipline automatically using add triggers in the pipline screen
