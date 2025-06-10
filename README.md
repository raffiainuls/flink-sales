## Flink Sales Streaming Data Pipeline 

#### Overview 
![flowchart_flink_sales_fix](https://github.com/user-attachments/assets/8edfb6e9-de14-469c-81e2-45a366475095)

This project is a Streaming data pipeline project that the output is a dashbord. This project using kafka for real-time data streaming and Apache Flink for ETL Streaming for execute query table, and then clickhouse for database, and grafane for creating dashbord.

#### Features 
- Producer Kafka: Python script generates table sales data and save it as csv
- Stream Data Generation: Python script that streaming or produce data into kafka
- Flink Python Job: Python Job Flink for execute table-table query
- Connector Kafka Connect: Connector clickhouse kafka connect that insert data from kafka into clickhouse
- Dashbord Grafana: Using Plugin clickhouse for datasource and make dashbord from data in clickhouse

### Technologies Used 
- Python
- Apache Kafka
- Apache Flink
- Connector Clickhouse Kafka Connect
- Clickhouse
- Grafana
- Docker


### Project Structure
<pre>  flink-sales/
   |-- clickhouse/                       # directory configurasi docker clickhouse
       |-- config.xml                    
       |-- schema.sql                    
       |-- setup apache superset on docker.txt                
       |-- zookeeper-servers.xml       
   |-- database/                   # directory data for this project 
       |-- df_branch.csv                    
       |-- df_cust.csv                    
       |-- df_employee.csv              
       |-- df_order_status.csv
       |-- df_payment_status.csv                    
       |-- df_payment_method.csv                    
       |-- df_product.csv              
       |-- df_promotions.csv
       |-- df_sales.csv                    
       |-- df_schedule.csv              
       |-- df_shipping_status.csv
       |-- list_file.txt         # this file is useful for producer.py can know which csv file is used for create datapipeline into kafka             
       |-- producer.py           # this python file will send data from csv file in list_file.txt to kafka topics 
   |-- flink-job/                 # directory volumes mapping for job flink 
       |-- branch_daily_finance_performance/      # directory lib function for table branch_daily_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- branch_finance_performanc/             # directory lib function for table branch_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- branch_monthly_finance_performance/    # directory lib function for table branch_monthly_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- branch_performance/   
            |-- __init__.py
            |-- lib.py
       |-- branch_weeakly_finance_performance/     # directory lib function for table branch_weeakly_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- daily_finance_performance/              # directory lib function for table daily_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- fact_employee/                          # directory lib function for table fact_employee 
            |-- __init__.py
            |-- lib.py
       |-- fact_sales/                             # directory lib function for table fact_sales
            |-- __init__.py
            |-- lib.py
       |-- helper/                                 # directory helper that containing function_funcition that leverage for this project
            |-- __init__.py
            |-- function.py
       |-- monthly_branch_performance/             # directory lib function for table monthly_branch_performance
            |-- __init__.py
            |-- lib.py
       |-- monthly_finance_performance/            # directory lib function for table monthly_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- product_performance/                    # directory lib function for table product_performance
            |-- __init__.py
            |-- lib.py
       |-- sum_transactions/                       # directory lib function for table sum_transactions
            |-- __init__.py
            |-- lib.py
       |-- tbl_branch/                             # directory lib function for table tbl_branch
            |-- __init__.py
            |-- lib.py
       |-- tbl_employee/                           # directory lib function for table tbl_employee
            |-- __init__.py
            |-- lib.py
       |-- tbl_product/                            # directory lib function for table tbl_product
            |-- __init__.py
            |-- lib.py
       |-- tbl_promotions/                         # directory lib function for table tbl_promotions
            |-- __init__.py
            |-- lib.py
       |-- tbl_sales/                              # directory lib function for table tbl_sales
            |-- __init__.py
            |-- lib.py
       |-- weeakly_finance_performance/            # directory lib function for table weeakly_finance_performance 
            |-- __init__.py
            |-- lib.py
       |-- init.py
       |-- branch_daily_finance_performance.py        # Python main executor for table branch_daily_finance_performance
       |-- branch_finance_performance.py              # Python main executor for table branch_finance_performance
       |-- branch_monthly_finance_performance.py      # Python main executor for table branch_monthly_finance_performance
       |-- branch_performance.py                      # Python main executor for table branch_performance
       |-- branch_weeakly_finance_performance.py      # Python main executor for table branch_weeakly_finance_performance
       |-- daily_finance_performance.py               # Python main executor for table daily_finance_performance
       |-- fact_employee.py                           # Python main executor for table fact_employee
       |-- fact_sales.py                              # Python main executor for table fact_sales
       |-- finance_performance.py                     # Python main executor for table finance_performance
       |-- monthly_branch_performance.py              # Python main executor for table monthly_branch_performance
       |-- monthly_finance_performance.py             # Python main executor for table monthly_finance_performance
       |-- sum_transactions.py                        # Python main executor for table sum_transactions
       |-- weeakly_finance_performance.py             # Python main executor for table weeakly_finance_performance
   |-- grafana-data/               # Directory volumes mapping grafana 
   |-- kafka/                      # Directory configurasi kafka
   |-- lib/                        # Directory plugins jar for docker kafka connect 
   |-- config.yaml                 # configurasi for stream.py
   |-- docker-compose.yaml         # docker-compose file 
   |-- dockerfile                  # dockerfile for image flink 
   |-- last_id_backup              # backup list_id file
   |-- last_id.txt                 # this file save last_id that most_recent create in stream.py 
   |-- stream.py                   # file python that create data streaming and send into kafka 
 </pre>


### Project Workflow 

1. Producer Kafka
   - python script (/flink-sales/database/producer.py) will read list file csv in list_file.txt that containing database tables that saves into csv, and then from the list file csv python script will access csv file and then produce data in file csv into kafka. table that send to kafka such as : ```tbl_order_status, tbl_payment_method, tb_payment_status, tbl_shipping_status, tbl_employee, tbl_promotions, tbl_sales, tbl_product, tbl_schedulle_emp, tbl_customers, and tbl_branch```
2. Stream Data Generation
   - This python script if it run will generate data and produce data into kafka topics table tbl_sales, this python script also have metrics thatis adjusted to the table in the csv file that was previously sent to kafka, so the generated data contains several calculations for several fields so that the data produced is not too random.
3. Flink Python Job
   - in directory /flink-sales/flink-job/  it is directory volumes mapping to container flink jobmanager in this directory there is all job that make table env for each table and execute query to insert data into the table env for each table.
4. Connector Clickhouse Kafka Connect
   - in directory /flink-sales/connector/ there is  some json file which is configuration connector that send data from kafka topics into clickhouse database
5. Dashbord Grafana
   - In this project i use simple dashbord with grafana because in grafana there is official plugins data source from clickhouse

### Instalation & Setup 
#### Prerequisites 
- Docker & Docker Compose
- Kafka & zookeeper (in this project i use kafka and zookeeper in local windows)
- Images Docker debezium, Apache Flink, Clickhouse, Grafana
- Python

### Steps 
1. clone this repository
   ```bash
   https://github.com/raffiainuls/flink-sales
2. prepare zookeeper & kafka first, and make sure your zookeeper and kafka allredy running correctly
3. start all container or up docker compose file
      ```bash
   docker-compose up
4. wait until the all container already running correctly, you can running ```/flink-sales/database/producer.py``` this file will send data in csv file that lists in list_file.txt into kafka
5. you can check is the topics already in kafka, you can use confluent control center for checking topics go to ```localhost:9091```
6. if all topics for each table already available in kafka you can run ```/flink-sales/stream.py``` this file will generate data streaming into topic tbl_sales in kafka. in this python script there is some calculations metrics for generate value in some field, so the value that produce not too random.
7. in this project all job for etl streaming are in the directory ```/flink-sales/flink-job/``` there is 13 where each job is a query that forms the table.
8.  where the work structure for each job will have a query tht will retrieve data from several tables, therefore before executing the query, an env table will be created first for the tables needed. For example for the fact_sales table, the env table will be created first for fact_sales itself, then an env table will also be created for several reference table such as tbl_sales, tbl_product, and tbl_branch. after that the fact_sales query will be executed
9.  the functions for each job will be in the directory of each job in the lib.py file and the functions for each job will be called in the main python file executor for each job in the flink-job directory the the file name matches the job name or table name.
10.  To carry out each job, we need to apply the job to flink in this way go to job manager bash
      ```bash
      docker exec -it jobmanager bash
11. and then apply job to jobmanager with this command
      ```bash
      flink run -py /opt/flink/job/fact_sales.py
12. the example above runs a job for fact_sales table, then do it for all existing jobs or tables.
13. after the all job alredy running correctly, we can check to the kafka topics there will be new topics for each job or table.
14. for next step we should sink our topic to the clickhouse with connector clickhouse in kafka connect but before postting the connector for each table, we must unsure that the tables are availabe on clickhouse, note for ddl for each table there are available in ```/flink-sales/ddl-table-clickhouse/``` 
15. post the connecotr on our kafka connect, use curl for post the connector or we can also use postman to post the connector, if use curl copy the command bellow
```bash
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @fact_sales_sink.json
```
16. file json configurations for each connector table already available in /flink-sales/connector-clickhouse
17. if the data for each table already available in clickhouse, means we have finished the data pipeline stageup to the clickhouse. we just have to create a table in grafana.
18. go to grafana web server and then add new plugins data source ```clickhouse plugins``` and then fill the configuration server clickhouse database.
19. if you up the container grafana in this repository maybe in your grafana already available dashbord for streaming data but if you dont't have one, you can make it yourself with your own style. 






  


