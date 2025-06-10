
CREATE TABLE branch_daily_finance_performance (
        name String,
        `date` DateTime64(3), 
        unit_sold Int32, 
        gmv Int64, 
        outcome Int64,
        revenue Int64,
        gross_profit Int64,
        nett_profit Int64
)ENGINE = ReplacingMergeTree()
order by (name, `date`);



create table branch_finance_performance (
	name String, 
	outcome Int64,
	gmv Int64,
	revenue Int64, 
	gross_profit Int64, 
	net_profit Int64
) ENGINE = ReplacingMergeTree()
order by name;


create table branch_monthly_finance_performance (
	name String, 
	`month` Date32,
	unit_sold Int32,
	gmv Int64,
	outcome Int64,
	revenue Int64,
	gross_profit Int64,
	nett_profit Int64
) ENGINE = ReplacingMergeTree()
order by (name, `month`);


create table branch_performance (
	branch_id Int32,
	branch_name String, 
	total_sales Int32,
	amount Int64
) ENGINE = ReplacingMergeTree()
order by branch_id;


create table branch_weeakly_finance_performance (
	name String, 
	`week` Date32,
	unit_sold Int32,
	gmv Int64,
	outcome Int64,
	revenue Int64,
	gross_profit Int64,
	nett_profit Int64
) ENGINE = ReplacingMergeTree()
order by (name, `week`);

create table daily_finance_performance (
	`date` Date32,
	unit_sold Int32,
	gmv Int64,
	outcome Int64,
	revenue Int64,
	gross_profit Int64,
	nett_profit Int64
) ENGINE = ReplacingMergeTree()
order by `date`;


CREATE TABLE fact_employee (
        id Int32,
        branch_id Int32, 
        name String, 
        salary Int64, 
        active boolean,
        address String, 
        phone String, 
        email String,
        created_at DateTime64(3)
) ENGINE = ReplacingMergeTree()
order by id;


create table monthly_branch_performance (
	branch_id Int32,
	branch_name String, 
	bulan Date32,
	total_sales Int32,
	amount Int64
)ENGINE = ReplacingMergeTree()
order by branch_id;

CREATE TABLE monthly_finance_performance (
        `month` Date32,
        unit_sold Int32,
        gmv Int64,
        outcome Int64,
        revenue Int64,
        gross_profit Int64,
        nett_profit Int64
    )ENGINE = ReplacingMergeTree()
order by (`month`);


CREATE TABLE product_performance (
        product_id Int32,
        product_name String, 
        jumlah_terjual Int32
    )ENGINE = ReplacingMergeTree()
order by (product_id);



CREATE TABLE weeakly_finance_performance (
        `week` Date32,
        unit_sold Int32, 
        gmv Int64, 
        outcome Int64, 
        revenue Int64,
        gross_profit Int64,
        nett_profit Int64
    )ENGINE = ReplacingMergeTree()
order by (`week`);









