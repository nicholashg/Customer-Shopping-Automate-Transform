CREATE TABLE table_m3 (
    "Customer ID" int,
	"Age" int,
	"Gender" varchar(225),
	"Item Purchased" varchar(225),
	"Category" varchar(225),
	"Purchase Amount (USD)" int,
	"Location" varchar(225),
	"Size" varchar(225),
	"Color" varchar(225),
	"Season" varchar(225),
	"Review Rating" float,
	"Subscription Status" varchar(225),
	"Payment Method" varchar(225),
	"Shipping Type" varchar(225),
	"Discount Applied" varchar(225),
	"Promo Code Used" varchar(225),
	"Previous Purchases" int,
	"Preferred Payment Method" varchar(225),
	"Frequency of Purchases" varchar(225)
);


copy table_m3 from 'D:\Hacktiv8\PHASE 2\MILESTONE 3\P2M3_nicholas_data_raw.csv' delimiter ',' CSV HEADER;

SELECT * from table_m3