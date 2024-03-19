

CREATE TABLE if not exists "Trading".Dim_Event(
  Event_Dim_ID int PRIMARY KEY, 
  Stock_ID int,
  Stock_Name varchar(50),
  Event_Date date,
  Event_Type varchar(50),
  Event_Description text,
  Current_Flag boolean,
  Effective_Timestamp timestamp,
  Expire_Timestamp timestamp
);

CREATE TABLE if not exists "Trading".Fact_Company_Event (
  FCE_ID int PRIMARY KEY, 
  Stock_ID int,
  Date_ID int,
  Event_Dim_ID int,
  FOREIGN KEY (Stock_ID) REFERENCES "Trading".Dim_Stock(Stock_ID), 
  FOREIGN KEY (Date_ID) REFERENCES "Trading".Dim_Date(Date_ID), 
  FOREIGN KEY (Event_Dim_ID) REFERENCES "Trading".Dim_Event(Event_Dim_ID) 
);


CREATE TABLE if not exists "Trading".Dim_Stock (
  Stock_ID int PRIMARY KEY, 
  Stock_Name varchar(50),
  Stock_Type varchar(50),
  Stock_Industry varchar(50),
  Stock_Industry_Prev varchar(50),
  Stock_Market varchar(50)
);


CREATE TABLE if not exists "Trading".Fact_Stock_Daily (
  FSD_ID int PRIMARY KEY,
  Stock_ID int,
  Date_ID int,
  Open_price decimal(10,2),
  Highest_Price decimal(10,2),
  Lowest_Price decimal(10,2),
  Volume int,
  Turnover decimal(10,2),
  Amplitude decimal(10,2),
  Percent_Of_Incre_Decre decimal(10,2),
  Amount_Of_Incre_Decre decimal(10,2),
  Turnover_Rate decimal(10,2),
  FOREIGN KEY (Stock_ID) REFERENCES "Trading".Dim_Stock(Stock_ID), 
  FOREIGN KEY (Date_ID) REFERENCES "Trading".Dim_Date(Date_ID) 
);


CREATE TABLE if not exists "Trading".Dim_Fundamental (
  DF_ID int PRIMARY KEY, 
  Stock_ID int,
  Book_Value_Per_Share decimal(10,2),
  PB_Ratio decimal(10,2),
  TTM_NET_EPS decimal(10,2),
  PE_Ratio decimal(10,2),
  Stock_Price decimal(10,2),
  Release_Date date
);


CREATE TABLE if not exists "Trading".Fact_Company_Fundamental (
  FCB_ID int PRIMARY KEY, 
  Stock_ID int,
  DF_ID int,
  Description text,
  FOREIGN KEY (Stock_ID) REFERENCES "Trading".Dim_Stock(Stock_ID), 
  FOREIGN KEY (DF_ID) REFERENCES "Trading".Dim_Fundamental(DF_ID) 
);

CREATE TABLE if not exists "Trading".Dim_Date (
  Date_ID int PRIMARY KEY, 
  Date date,
  Year int,
  Month int,
  Day int,
  Weekday int,
  Quarter int
);


CREATE TABLE if not exists "Trading".Dim_Minute (
  Minute_ID int PRIMARY KEY, 
  Hour int,
  Minute int
);
ALTER TABLE "Trading".Dim_Minute
ADD COLUMN Date varchar(50);

CREATE TABLE if not exists "Trading".Fact_Stock_Minute (
  FSM_ID int PRIMARY KEY, 
  Stock_ID int,
  Minute_ID int,
  Open_Price decimal(10,2),
  Highest_Price decimal(10,2),
  Lowest_Price decimal(10,2),
  Volume int,
  Stock_Name varchar(50),
  FOREIGN KEY (Stock_ID) REFERENCES "Trading".Dim_Stock(Stock_ID), 
  FOREIGN KEY (Minute_ID) REFERENCES "Trading".Dim_Minute(Minute_ID) 
);

select * from "Trading".Dim_minute;

