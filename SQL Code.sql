--1. Tổng quan tình hình doanh thu

--Bước 1: Tạo CTE revenue để lấy các trường:
--order_id, order_status, order_purchase_timestamp (Bảng orders_dataset)

with revenue as (
    select
        orders.order_id,
        order_status,
        order_purchase_timestamp,
        payments.total_revenue
    from 
        orders_dataset orders
-- Bước 2: Join với Subquery bảng payments:
-- Dữ liệu của  này lấy từ orders_payments_dataset với 2 trường: 
-- order_id và sum(payment_value) = total_revenue được group by theo order_id 
-- Inner join orders_dataset và payments với khóa order_id Vì bảng orders_payments_dataset có 1 đơn hàng không phát sinh thanh toán    
    inner join (
        select 
            order_id,
            sum(payment_value) as total_revenue
        from orders_payments_dataset
        group by 
            order_id
        ) as payments
    on payments.order_id=orders.order_id
-- Lấy các đơn hàng được đặt đến hết tháng 9 năm 2018 (Theo yêu cầu đề bài)
    where orders.order_purchase_timestamp<'2018-10-01'
)

select top 10
    *
from 
    revenue

--> Bảng mới ghi nhận dữ liệu doanh thu của 99436 đơn hàng thỏa mãn các điều kiện trên 


--2. Doanh thu và số lượng sản phẩm theo Category
with category as (
    select 
        items.order_id,
        items.order_item_id,
        items.product_id,
        trans.product_category_name_translation,
        items.price,
        items.freight_value
    from 
        orders_items_dataset items
    left join 
        products_dataset prod
        on prod.product_id=items.product_id
    left join 
        product_category_name_translation trans 
        on trans.product_category_name=prod.product_category_name
)
select top 10
    *
from 
    category

--3. Các hình thức thanh toán

select 
    payment_type,
    count(order_id) as payment_qty,
    sum(payment_value) as total_revenue
from    
    orders_payments_dataset
group by 
    payment_type
order by 
    total_revenue desc


--4. Mức độ hài lòng của khách hàng
select top 10
    review_score,
    count(review_id) as count_review
from 
    orders_reviews_dataset
group by 
    review_score
order by    
    review_score desc


--AREAS AND SOLUTIONS NEED TO BE IMPROVED
--1. Tính toán Average Order Value (AOV) theo tháng/năm
with revenue as (
    select
        orders.order_id,
        orders.order_status,
        orders.order_purchase_timestamp,
        year(orders.order_purchase_timestamp) as[year],
        month(orders.order_purchase_timestamp) as [month],
        payments.total_revenue
    from 
        orders_dataset orders
    inner join (
        select 
            order_id,
            sum(payment_value) as total_revenue
        from orders_payments_dataset
        group by 
            order_id
        ) as payments
    on payments.order_id=orders.order_id
    where orders.order_purchase_timestamp<'2018-10-01'
)

select 
    year,
    month,
    count(order_id) as order_qty,
    sum(total_revenue) as total_revenue,
    (sum(total_revenue)/count(order_id)) as average_order_value
from 
    revenue
group by 
    [year],
    [month]
order by    
    [year],
    [month]

-- Để tăng AOV có 2 cách: tăng order_qty, tăng total_revenue, 
--> Chọn tăng total_revenue 
--> total revenue có 2 yếu tố: số lượng sản phẩm trong 1 đơn hàng * Giá của sản phẩm 
--> Giá sản phẩm không thay đổi được
--> Thay đổi số lượng sản phẩm trong 1 đơn hàng
--> Thống kê về số lượng sản phẩm trong 1 đơn hàng toàn bằng 1 (90%)
--> Đưa ra giải pháp:
-- Từ phía nhà bán hàng: Upsales, CrossSales
-- Từ phía sàn TMĐT


--2. Tìm hiểu các đơn hàng từ 1-2 sao
with review as(
    select 
        reviews.review_id,
        reviews.order_id,
        orders.order_status,
        reviews.review_score, 
        reviews.review_comment_title,
        reviews.review_comment_message
    from 
        orders_reviews_dataset reviews 
        left join 
            orders_dataset orders
        on orders.order_id=reviews.order_id
    where review_score= 1
        or review_score=2
)
select
    review.review_id,
    review.order_id,
    items.order_qty,
    review_score,
    review_comment_title,
    review_comment_message
from 
    review
    left join 
        (select 
            order_id,
            max(order_item_id) as order_qty
        from orders_items_dataset
        group by order_id) as items 
    on items.order_id=review.order_id

--3. Khu vực địa lý nào có khách hàng đem lại doanh thu cao nhất?
with region as(
    select 
        orders.*,
        cust.customer_city,
        cust.customer_state
    from    
        orders_dataset orders
    left join customers_dataset cust 
        on cust.customer_id=orders.customer_id
)

select 
    region.customer_city,
    region.customer_state,
    sum(temp_a.total_revenue) as 'Total'
from 
    region
    left join (select 
                order_id,
                sum(payment_value) as total_revenue
            from orders_payments_dataset
            group by order_id) as temp_a
    on temp_a.order_id=region.order_id
group by 
    region.customer_city,
    region.customer_state
order by 
    Total desc


