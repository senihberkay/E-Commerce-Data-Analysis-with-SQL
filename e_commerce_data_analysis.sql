
-- Using the columns of “market_fact”, “cust_dimen”, “orders_dimen”, “prod_dimen”, “shipping_dimen”, created a new table named as “combined_table”.

create view [combined_view] 
as
(
	select A.Ord_id, A.Prod_id, A.Ship_id, A.Cust_id, A.Sales, A.Discount, A.Order_Quantity, A.Product_Base_Margin,
	       B.Order_ID, B.Ship_Date, B.Ship_Mode,
		   C.Customer_Name, C.Province, C.Region, C.Customer_Segment,
		   D.Order_Date, D.Order_Priority,
		   E.Product_Category, E.Product_Sub_Category

	from market_fact A
	left join shipping_dimen B on A.Ship_id = B.Ship_id
	left join cust_dimen C on A.Cust_id = C.Cust_id
	left join orders_dimen D on A.Ord_id = D.Ord_id
	left join prod_dimen E on A.Prod_id = E.Prod_id
);


SELECT  *
INTO    combined_table
FROM    combined_view



-- Find the top 3 customers who have the maximum count of orders

select top 3 Cust_id, Customer_Name, count(Order_Quantity) count_orders 
from combined_table 
group by Cust_id, Customer_Name
order by count_orders DESC;



-- Created a new column at combined_table as DaysTakenForDelivery that contains 
-- the date difference of Order_Date and Ship_Date.

alter table combined_table
add DaysTakenForDelivery int


update combined_table
set DaysTakenForDelivery = datediff(DAY, Order_Date, Ship_Date)
from combined_table



-- The customer whose order took the maximum time to get delivered.

select top (1) Cust_id, Customer_Name, DaysTakenForDelivery
from combined_table
order by DaysTakenForDelivery DESC



-- Count the total number of unique customers in January and 
-- how many of them came back every month over the entire year in 2011


select month(Order_Date) [Month], datename(month, Order_Date) [Month_name], count(distinct Cust_id) cust_count
from combined_table where Cust_id
in (
	select distinct Cust_id
	from combined_table
	where month(Order_Date) = 1 and year(Order_date) = 2011
) 
and year(Order_date) = 2011
 group by month(Order_Date), DATENAME(MONTH, Order_Date)
 order by [Month]



 -- For each user the time elapsed between the first purchasing and the third purchasing, 
 -- in ascending order by Customer ID.

SELECT  
		distinct convert(int, SUBSTRING(Cust_id, 6, len(Cust_id))) AS Customer_ID,
		Order_Date as Third_Order_Date,
		First_Order_Date,
		DATEDIFF(day, First_Order_Date, Order_Date) Days_Elapsed
FROM	
		(
		SELECT	Cust_id, Order_Date,
				MIN (Order_Date) OVER (PARTITION BY Cust_id) First_Order_Date,
				DENSE_RANK () OVER (PARTITION BY Cust_id ORDER BY Order_Date) dense_number
		FROM	combined_table
		) A
WHERE	dense_number = 3
Order By Customer_ID ASC;



-- customers who purchased both product 11 and product 14, 
-- as well as the ratio of these products to the total number of products purchased by the customer.


-- with t1 as (
	select distinct convert(int, SUBSTRING(Cust_id, 6, len(Cust_id))) AS Customer_ID,
		CAST (1.0*sum(case when Prod_id = 'Prod_11' then Order_Quantity else 0  end)/sum(Order_Quantity) AS NUMERIC (3,2)) AS Ratio_P11,
		CAST (1.0*sum(case when Prod_id = 'Prod_14' then Order_Quantity else 0  end)/sum(Order_Quantity) AS NUMERIC (3,2)) AS Ratio_P14
		-- sum(case when Prod_id = 'Prod_11' then Order_Quantity else 0  end) P11,
		-- sum(case when Prod_id = 'Prod_14' then Order_Quantity else 0 end) P14,
		-- sum(Order_Quantity) Total_Prod
	from combined_table
	group by Cust_id
	HAVING
		SUM (CASE WHEN Prod_id = 'Prod_11' THEN Order_Quantity ELSE 0 END) >= 1 AND
		SUM (CASE WHEN Prod_id = 'Prod_14' THEN Order_Quantity ELSE 0 END) >= 1
--)
--select Customer_ID, Total_Prod,
--		CAST (1.0*P11/Total_Prod AS NUMERIC (3,2)) AS Ratio_P11,
--		CAST (1.0*P14/Total_Prod AS NUMERIC (3,2)) AS Ratio_P14
--from t1




-- Created a 'view' that keeps visit logs of customers on a monthly basis. 
-- (For each log, three field is kept: Cust_id, Year, Month)

create view visit_log as
(
	select convert(int, SUBSTRING(Cust_id, 6, len(Cust_id))) AS Customer_ID,
	year(Order_Date) [Year], Month(Order_Date) [Month]
	from combined_table
	group by Cust_id, YEAR(Order_Date) , MONTH(Order_Date)
);



-- Created a 'view' that keeps the number of monthly visits by users. 
-- (Showed separately all months from the beginning business)

create view visit_count as
(
	select convert(int, SUBSTRING(Cust_id, 6, len(Cust_id))) AS Customer_ID,
	year(Order_Date) [Year],
	Month(Order_Date) [Month], 
	count(*) over(partition by Cust_id order by convert(int, SUBSTRING(Cust_id, 6, len(Cust_id)))) count_log
	from combined_table
);



-- For each visit of customers, created the next month of the visit as a separate column.

create  view next_visit_log as
(
	select *, lead(current_month, 1) over(partition by  Customer_ID  order by current_month) next_month_visit
	from
	(
		select *,  DENSE_RANK() over(order by [Year], [Month]) current_month
		from visit_log
		) t
);



-- Calculated the monthly time gap between two consecutive visits by each customer.

create view visit_gap as
(
	select *, next_month_visit - current_month cust_visit_gap
	from next_visit_log
);

-- Categorized customers using average time gaps. 

with cte as
(
	select Customer_ID, avg(cust_visit_gap) avg_visit_gap
	from visit_gap
	group by  Customer_ID
)

select Customer_ID,
		case
			when avg_visit_gap >= 1 then 'Regular'
			when avg_visit_gap is NULL then 'Churn'
		end as visit_freq
from cte

-- Month-Wise Retention Rate
-- Finded month-by-month customer retention rate since the start of the business.

select * from visit_gap

create view current_count
as
(
	select *, 
		count(Customer_ID) over(partition by current_month) current_count_customer
	from visit_gap
	where current_month > 1
);

create view next_count
as
(
	select *,
		count(Customer_ID) over(partition by next_month_visit) next_count_customer
	from visit_gap
	where cust_visit_gap = 1
);


with t1 as(
	select A.Customer_ID, A.next_month_visit, A.current_count_customer, B.next_count_customer
	from current_count A, next_count B
	where A.next_month_visit = B.current_month
	and A.cust_visit_gap =  1
)
select distinct cast(1.0 * next_count_customer/current_count_customer  as numeric (3,2)) as Month_Wise_Retention_Rate
from t1

