
# Important Detail

This Whole case study was done on a single local machine. So the expanation to run below was made with that in mind.

# DE_CaseStudy
Data Engineering Case Study

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Jesse Tackett - PerScholas pySpark Case Study - October 25, 2019

======================Overhead:======================

A) Start Kafka

	Make sure Topics are Reset
	
	Start Zookeeper
	
	Start Kafka server
	
	Keep it running in the background
	
B) Run MainTransfer

	Run code in command prompt
	
C) Start Jupyter

	Upload jupyter code
	
D) Run Visualization

	Run blocks from the top -> down
	
E) Enjoy!

======================Details:======================

Transfer (Req.1.1 through 2.3)
-------- ---------------------
Assuming:
	* You already have Kafka, Spark, MongoDB, MariaDB, and Jupyter installed
	* You already have the CDW_SAPP tables inside MariaDB
	* You already cleared your Kafka Topics/Logs for a clean start

To run this Case Study you will first need to:

* Open command prompt and run this command to start Zookeeper Server:

	'zookeeper-server-start.bat C:\kafka\config\zookeeper.properties'
	
			OR
			
	'zookeeper-server-start.bat C:\"Your zookeeper.properties file destination"'

* Open a second command prompt and run this command to start Kafka Server:

	'kafka-server-start.bat C:\kafka\config\server.properties'
	
			OR
			
	'kafka-server-start.bat C:\"Your server.properties file destination"'

* Open a third command prompt and run this command to start the Transfer Process:

	'cd C:\Downloads\TackettJesse_CaseStudy\DataTransfer'

* Then this command in the third command prompt:

	'python TackettJesse_MainTransfer.py'
	
This will run selection process on all the data and what you want to transfer when.
Just follow the prompts for input and exit the program when you are finished.

Visualization (Req. 2.4)
------------- ----------
Once all the data in loaded into mongo start this process.

* Start and run the Jupyter Application
	It should open it's own command prompt and a new tab in your browser

* Then upload the file:
	'TackettJesse_Visualization.ipynb'

* Run the Notebook you just uploaded
	A new tab should open in your browser showing all the code and results

OPTIONAL:
	There is the raw python file for visualization if you would like it.

Run each code block within the notebook from top to bottom and then enjoy the results!
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


Thank you for looking at my Case Study, have a wonderful day!
