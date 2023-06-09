# Nakheel Data Engineer Assignment By Nikunj Solanki

The solution has been implemented with two approaches.
- Hive
- MySQL

As per the problem statement, I suppose to use Relational DB but if we think more broadly, in the future this solution may degrade the performance because of infra and resource constraints and huge data volume. So, to mitigate such futuristic bottlenecks, I have chosen Hive as one of the alternate solutions.

In this repo, the below directories are there
- mysql
- hive
- conf
- inputFiles

#### mysql directory

There are 2 Python files which contains the solution.
1. DataLoader.py -> This file reads CSV files from the local machine path and writes data into MySQL "assignment" database. 
2. DataProcessor.py -> This file reads MySQL db, applies filter + join + custom logic, and finally writes to other tables in the same database. As this is a sample project and assignment, you may observe multiple show()/count() actions being used which are kept intentionally. In the case of UAT/PROD, such developer actions should not be there.
3. outputScreenshot -> This directory contains screenshots and logs of the local system and local MySql DB after executing the queries. 


#### hive directory

There are 2 Python files which contains the solution.
1. DataLoader.py -> This file reads CSV files from the local machine path and writes data into Hive. By default local setup has taken derby for metadata.  
2. DataProcessor.py -> This file reads Hive tables, applies filter + join + custom logic, and finally writes to other tables in the same Hive internal tables. As this is a sample project and assignment, you may observe multiple show()/count() actions being used which are kept intentionally. In the case of UAT/PROD, such developer actions should not be there.
3. output_query1_query2.log -> Hive approach console output is pasted in this file.

#### conf directory

This directory contains MySQL DB details like URL, User, etc. This is being read from the MySQL DataLoader.py script.

#### inputFiles directory

This directory contains the files with sample data that was shared and being referred in both approaches. **Note: There is no change in sample data.**


#### NOTE
Reporting tools like PowerBI can connect MySQL and Hive also visualize the data into a report/dashboard and that is the reason, query outputs are being written again into separate tables. Also, there is "Trino" which we can use to query Hive tables to improve the performance. So in the future, if the data volume increases, the application will be enough robust and faster to handle the load.
