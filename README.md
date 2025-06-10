## Flink Sales Streaming Data Pipeline 

#### Overview 
![flowchart_flink_sales_fix](https://github.com/user-attachments/assets/8edfb6e9-de14-469c-81e2-45a366475095)

This project is a Streaming data pipeline project that the output is a dashbord. This project using kafka for real-time data streaming and Apache Flink for ETL Streaming for execute query table, and then clickhouse for database, and grafane for creating dashbord.

#### Features 
- Data Generation: Python script generates table sales data and save it as csv
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
  

  


