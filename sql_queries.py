import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_sandp500_companieslist_table_drop = "DROP TABLE IF EXISTS staging_sandp500_companieslist"
staging_sandp500_index_info_table_drop = "DROP TABLE IF EXISTS staging_sandp500_index_info"
staging_sandp500_stockinfo_table_drop = "DROP TABLE IF EXISTS staging_sandp500_stockinfo"
staging_convid19_statistic_table_drop = "DROP TABLE IF EXISTS staging_convid19_statistic"
covid19_table_drop = "DROP TABLE IF EXISTS covid19"
time_table_drop = "DROP TABLE IF EXISTS time"
sandp_index_table_drop = "DROP TABLE IF EXISTS sandp_index"
sandp_500_company_list_table_drop = "DROP TABLE IF EXISTS sandp_500_company_list"
stock_info_table_drop = "DROP TABLE IF EXISTS stock_info"
covid19_impact_on_stock_table_drop = "DROP TABLE IF EXISTS covid19_impact_on_stock"


# CREATE TABLES

staging_sandp500_companieslist_table_create= ("""
CREATE TABLE staging_sandp500_companieslist
(ticker varchar(256) NOT NULL, company_name varchar(256) NOT NULL, sector varchar(256) NOT NULL, industry varchar(256), CONSTRAINT company_pkey PRIMARY KEY (ticker))      
""")

staging_sandp500_index_info_table_create = ("""
CREATE TABLE staging_sandp500_index_info
(cal_day INTEGER,"open" float, "high" float, "low" float, "close" float, adjclose float,"volume" float,"date" date,year INTEGER, month INTEGER,weekofyear INTEGER, weekdayname VARCHAR(100))      
""")

staging_sandp500_stockinfo_table_create= ("""
CREATE TABLE staging_sandp500_stockinfo(
cal_day INTEGER NOT NULL, ticker varchar(256) NOT NULL,"high" FLOAT, "low" FLOAT, "open" FLOAT,"close" FLOAT,"volume" FLOAT,adjclose FLOAT, "date" date,"year" INTEGER,"month" INTEGER NOT NULL,weekofyear INTEGER,weekdayname varchar(256))
""")


staging_convid19_statistic_table_create= ("""
CREATE TABLE staging_convid19_statistic(
country_iso_code varchar(256) NOT NULL, 
continent varchar(256), 
location varchar(256), 
cal_day INTEGER,
total_cases FLOAT,
new_cases FLOAT,
total_deaths FLOAT,
new_deaths FLOAT NOT NULL, 
"date" date,
year INTEGER,
month INTEGER NOT NULL,
weekofyear INTEGER,
weekdayname varchar(256))
""")


covid19_table_create = ("""
CREATE TABLE covid19(
    "date" date,
    country_iso_code varchar(256) NOT NULL, 
    continent varchar(256), 
    location varchar(256), 
    total_cases FLOAT,
    new_cases FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT NOT NULL)    
""")

time_table_create = ("""
CREATE TABLE time
(
    "date" date,
    "year" INTEGER,
    "month" INTEGER NOT NULL,
    weekofyear INTEGER,
    weekdayname varchar(256))
""")



sandp_index_table_create = ("""
CREATE TABLE sandp_index
(
   index_open float, 
   index_high float, 
   index_low float, 
   index_close float, 
   index_volume float,
   index_date date)
""")


sandp_500_company_list_table_create = ("""
CREATE TABLE sandp_500_company_list
(
   ticker varchar(256) NOT NULL, 
   company_name varchar(256) NOT NULL, 
   sector varchar(256) NOT NULL, 
   industry varchar(256))
""")


stock_info_table_create = ("""
CREATE TABLE stock_info
(
   ticker varchar(256) NOT NULL,
   "high" FLOAT, 
   "low" FLOAT, 
   "open" FLOAT,
   "close" FLOAT,
   "volume" FLOAT,
   "date" date)
""")



covid19_impact_on_stock_table_create = ("""
CREATE TABLE covid19_impact_on_stock
(
    "date" date,
    ticker varchar(256) NOT NULL,
    company_name varchar(256) NOT NULL,
    "month" INTEGER NOT NULL,
    weekdayname varchar(256),
    "open" FLOAT,
    "high" FLOAT, 
    "low" FLOAT, 
    index_open float, 
    index_high float, 
    index_low float, 
    new_cases FLOAT,
    total_cases FLOAT,
    new_deaths FLOAT NOT NULL, 
    total_deaths FLOAT)
""")








##### STAGING TABLES



staging_sandp500_companieslist_copy = ("""copy staging_sandp500_companieslist 
                          from {} 
                          iam_role {}
                          CSV
                          IGNOREHEADER 1
                          region 'eu-north-1';
                          
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_sandp500_index_info_copy = ("""copy staging_sandp500_index_info 
                          from {} 
                          iam_role {}
                          IGNOREHEADER 1
                          CSV
                          region 'eu-north-1';
                      """).format(config.get('S3','SNPINDEX'), config.get('IAM_ROLE', 'ARN'))

staging_sandp500_stockinfo_copy = ("""copy staging_sandp500_stockinfo 
                          from {} 
                          iam_role {}
                          CSV
                          IGNOREHEADER 1
                          region 'eu-north-1';
                      """).format(config.get('S3','SNPALL'), config.get('IAM_ROLE', 'ARN'))


staging_convid19_statistic_copy = ("""copy staging_convid19_statistic 
                          from {} 
                          iam_role {}
                          CSV
                          IGNOREHEADER 1
                          region 'eu-north-1';
                      """).format(config.get('S3','COVID'), config.get('IAM_ROLE', 'ARN'))


#### FINAL TABLES

covid19_table_insert = ("""
INSERT INTO covid19 (date,country_iso_code,location,total_cases,new_cases,total_deaths,new_deaths)
SELECT DISTINCT  
        date,
        country_iso_code,
        location, 
        total_cases,
        new_cases,
        total_deaths,
        new_deaths
FROM staging_convid19_statistic    
""")
    
time_table_insert = ("""
INSERT INTO time ( date,year,month,weekofyear,weekdayname)
SELECT DISTINCT 
    date,
    year,
    month,
    weekofyear,
    weekdayname
FROM staging_sandp500_stockinfo   
""")


sandp_index_table_insert = ("""
INSERT INTO sandp_index (index_open, index_high, index_low, index_close, index_volume,index_date)
SELECT DISTINCT  
    "open", 
    "high", 
    "low", 
    "close", 
    "volume",
    "date"
FROM staging_sandp500_index_info
""")


sandp_500_company_list_table_insert = ("""
INSERT INTO sandp_500_company_list (ticker, company_name, sector, industry)
SELECT DISTINCT  
    ticker, 
    company_name, 
    sector, 
    industry
FROM staging_sandp500_companieslist
""")


stock_info_table_insert = ("""
INSERT INTO stock_info (ticker,"high","low","open","close","volume","date")
SELECT DISTINCT  
    ticker, 
    "high", 
    "low", 
    "open",
    "close",
    "volume",
    "date"
FROM staging_sandp500_stockinfo
""")



covid19_impact_on_stock_table_insert = ("""
INSERT INTO covid19_impact_on_stock ("date",ticker,company_name, "month",weekdayname,"open","high","low",index_open,index_high,index_low,new_cases,total_cases,new_deaths,total_deaths)
SELECT t.date,
stk_info.ticker,
complist.company_name,
t.month,
t.weekdayname,
stk_info."open",
stk_info.high,
stk_info.low,
snpidx.index_open,
snpidx.index_high,
snpidx.index_low,
c.new_cases,
c.total_cases,
c.new_deaths,
c.total_deaths
FROM stock_info AS stk_info
JOIN time t ON t.date=stk_info.date
JOIN sandp_500_company_list complist ON complist.ticker=stk_info.ticker
JOIN sandp_500_company_list s500_list ON s500_list.ticker=stk_info.ticker
JOIN covid19 c ON c.date=stk_info.date
JOIN sandp_index snpidx ON snpidx.index_date=stk_info.date
""")


create_table_queries = [staging_sandp500_companieslist_table_create,staging_sandp500_index_info_table_create, staging_sandp500_stockinfo_table_create,staging_convid19_statistic_table_create,covid19_table_create,time_table_create,sandp_index_table_create,sandp_500_company_list_table_create,stock_info_table_create,covid19_impact_on_stock_table_create]
drop_table_queries =[staging_sandp500_companieslist_table_drop,staging_sandp500_index_info_table_drop,staging_sandp500_stockinfo_table_drop,staging_convid19_statistic_table_drop, covid19_table_drop,time_table_drop,sandp_index_table_drop, sandp_500_company_list_table_drop,stock_info_table_drop,covid19_impact_on_stock_table_drop]
copy_table_queries=[staging_sandp500_companieslist_copy,staging_sandp500_index_info_copy,staging_sandp500_stockinfo_copy,staging_convid19_statistic_copy]
insert_table_queries = [covid19_table_insert,time_table_insert,sandp_index_table_insert,sandp_500_company_list_table_insert,stock_info_table_insert,covid19_impact_on_stock_table_insert]
