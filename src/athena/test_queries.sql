-- Test Query 1: Basic aggregation
SELECT
    neighbourhood_group,
    COUNT(*) as listing_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM
    listings
GROUP BY
    neighbourhood_group
ORDER BY
    listing_count DESC;

-- Test Query 2: Time-based analysis
SELECT
    year,
    month,
    COUNT(*) as new_listings,
    AVG(price) as avg_price
FROM
    listings
WHERE
    year >= 2019
GROUP BY
    year,
    month
ORDER BY
    year,
    month;

-- Test Query 3: Room type analysis
SELECT
    room_type,
    COUNT(*) as listing_count,
    AVG(price) as avg_price,
    AVG(minimum_nights) as avg_min_nights,
    AVG(number_of_reviews) as avg_reviews
FROM
    listings
GROUP BY
    room_type
ORDER BY
    listing_count DESC;

-- Test Query 4: Price distribution by location
SELECT
    neighbourhood_group,
    neighbourhood,
    COUNT(*) as listing_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY
            price
    ) as median_price,
    AVG(price) as mean_price
FROM
    listings
GROUP BY
    neighbourhood_group,
    neighbourhood
HAVING
    COUNT(*) > 10
ORDER BY
    median_price DESC;

-- Test Query 5: Review analysis
SELECT
    year,
    month,
    COUNT(DISTINCT id) as active_listings,
    SUM(number_of_reviews) as total_reviews,
    AVG(number_of_reviews) as avg_reviews_per_listing
FROM
    listings
WHERE
    year >= 2019
GROUP BY
    year,
    month
ORDER BY
    year,
    month;