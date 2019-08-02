DROP TABLE IF EXISTS review;
CREATE EXTERNAL TABLE IF NOT EXISTS review
(
marketplace string, 
  customer_id string, 
  review_id string, 
  product_id string, 
  product_parent string, 
  product_title string, 
  product_category string,
  star_rating int, 
  helpful_votes int, 
  total_votes int, 
  vine string, 
  verified_purchase string, 
  review_headline string, 
  review_body string, 
  review_date string)
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY '\t' 
LOCATION '/user/maria_dev/final/amazon';

select
    t1.year,
    t1.character,
    count(*) num_reviews
from
    (
    select
        substr(review_date,1,4) year,
        regexp_extract(lower(product_title), 
        '(mickey|donald duck|goofy|minnie|pluto|winnie the pooh|snow white|jack sparrow|tinkerbell|stitch| nala|oswald the lucky rabbit|ariel|baymax|cinderella| belle|alice in wonder lbuzz lightyear|rapunzeldaisy)'
        ,1) character

    from
        review
    where
        product_category = 'Baby'
    ) t1

where 
    length(t1.character) > 0 and
    t1.character in ('minnie','winnie the pooh','mickey','stitch','tinkerbell','ariel','cinderella','belle','snow white','nala') -- top 10 characters
    
group by
    t1.year,
    t1.character
