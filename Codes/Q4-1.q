DROP TABLE IF EXISTS disney_top5;
CREATE EXTERNAL TABLE IF NOT EXISTS disney_top5
(
  product_category string,
  product_id string, 
  product_title string, 
  character string,
  rank int,
  rating int,
  review_headline string,
  review_body string
  )
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY '\t' 
LOCATION '/user/maria_dev/final/amazon/amazon_top5_all';

insert overwrite table disney_top5 
select
    t4.product_category,
    t4.product_id,
    t4.product_title,
    t4.character,
    t4.rank,
    t4.rating,
    t5.review_headline,
    t5.review_body
from
(
    select
        t3.product_category,
        t3.product_id,
        concat(t3.product_category, '_', t3.product_id) product_unique_id,
        t3.product_title,
        t3.character,
        t3.rank,
        t3.rating,
        t3.num_reviews
    from
    (
        select
            t2.product_category,
            t2.product_id,
            t2.product_title,
            t2.character,
            t2.rating,
            t2.num_reviews,
            dense_rank() over(partition by t2.product_category order by t2.rating desc) rank
        from
        (
            select
                t1.product_category,
                t1.product_id,
                t1.product_title,
                t1.character,
                avg(t1.star_rating) rating,
                count(*) num_reviews
            from
                (
                select
                    product_category,
                    product_id,
                    product_title,
                    regexp_extract(lower(product_title), 
                    '(mickey|donald duck|goofy|minnie|pluto|winnie the pooh|snow white|jack sparrow|tinkerbell|stitch| nala|oswald the lucky rabbit|ariel|baymax|cinderella| belle|alice in wonder lbuzz lightyear|rapunzeldaisy)'
                    ,1) character,
                    star_rating

                from
                    review
                where
                    substr(review_date,1,4) >= '2010' and -- Restrict year to 2010 until 2015
                    substr(review_date,1,4) <= '2015'
                ) t1

            where 
                length(t1.character) > 0
            group by
                t1.product_category,
                t1.product_id,
                t1.product_title,
                t1.character
        ) t2
        where
            t2.num_reviews > 5
    ) t3
    where
        t3.rank <= 5
) t4
inner join
review t5
on
t4.product_unique_id = concat(t5.product_category, '_', t5.product_id) ;



    