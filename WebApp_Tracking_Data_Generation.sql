-- Web/App Tracking Data Generator for Looker Dashboard
-- This query generates randomized tracking data for 2 months (October & November 2024)
-- Includes user behavior, conversions, retention, and segmentation data

WITH date_range AS (
  -- Generate dates for Oct-Nov 2024
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY('2024-10-01', '2024-11-30')) AS date
),

user_segments AS (
  -- Define user segments with realistic distribution
  SELECT segment, weight FROM UNNEST([
    STRUCT('New' AS segment, 0.4 AS weight),
    STRUCT('Repeat' AS segment, 0.35 AS weight),
    STRUCT('NTU' AS segment, 0.25 AS weight)  -- Non-Transacting Users
  ])
),

platforms AS (
  -- Platform distribution
  SELECT platform, weight FROM UNNEST([
    STRUCT('Web' AS platform, 0.45 AS weight),
    STRUCT('Mobile App' AS platform, 0.35 AS weight),
    STRUCT('Tablet' AS platform, 0.20 AS weight)
  ])
),

verticals AS (
  -- Business verticals
  SELECT vertical, weight FROM UNNEST([
    STRUCT('E-commerce' AS vertical, 0.30 AS weight),
    STRUCT('Food Delivery' AS vertical, 0.25 AS weight),
    STRUCT('Travel' AS vertical, 0.20 AS weight),
    STRUCT('Finance' AS vertical, 0.15 AS weight),
    STRUCT('Entertainment' AS vertical, 0.10 AS weight)
  ])
),

funnel_stages AS (
  -- User journey stages
  SELECT stage, stage_order FROM UNNEST([
    STRUCT('Landing' AS stage, 1 AS stage_order),
    STRUCT('Browse' AS stage, 2 AS stage_order),
    STRUCT('Add to Cart' AS stage, 3 AS stage_order),
    STRUCT('Checkout' AS stage, 4 AS stage_order),
    STRUCT('Payment' AS stage, 5 AS stage_order),
    STRUCT('Success' AS stage, 6 AS stage_order)
  ])
),

base_data AS (
  SELECT 
    date,
    -- Generate random user IDs
    CONCAT('user_', CAST(FLOOR(RAND() * 100000) + 1 AS STRING)) AS user_id,
    -- Random session ID
    CONCAT('session_', CAST(FLOOR(RAND() * 1000000) + 1 AS STRING)) AS session_id,
    -- Weighted random selection for segments
    CASE 
      WHEN RAND() <= 0.4 THEN 'New'
      WHEN RAND() <= 0.75 THEN 'Repeat' 
      ELSE 'NTU'
    END AS user_segment,
    -- Weighted random selection for platforms
    CASE 
      WHEN RAND() <= 0.45 THEN 'Web'
      WHEN RAND() <= 0.80 THEN 'Mobile App'
      ELSE 'Tablet'
    END AS platform,
    -- Weighted random selection for verticals
    CASE 
      WHEN RAND() <= 0.30 THEN 'E-commerce'
      WHEN RAND() <= 0.55 THEN 'Food Delivery'
      WHEN RAND() <= 0.75 THEN 'Travel'
      WHEN RAND() <= 0.90 THEN 'Finance'
      ELSE 'Entertainment'
    END AS vertical,
    -- Random funnel stage
    CASE 
      WHEN RAND() <= 0.25 THEN 'Landing'
      WHEN RAND() <= 0.45 THEN 'Browse'
      WHEN RAND() <= 0.65 THEN 'Add to Cart'
      WHEN RAND() <= 0.80 THEN 'Checkout'
      WHEN RAND() <= 0.95 THEN 'Payment'
      ELSE 'Success'
    END AS funnel_stage,
    -- Random impressions (higher for earlier stages)
    CASE 
      WHEN RAND() <= 0.25 THEN FLOOR(RAND() * 20) + 1  -- Landing: 1-20
      WHEN RAND() <= 0.45 THEN FLOOR(RAND() * 15) + 1  -- Browse: 1-15
      WHEN RAND() <= 0.65 THEN FLOOR(RAND() * 10) + 1  -- Add to Cart: 1-10
      WHEN RAND() <= 0.80 THEN FLOOR(RAND() * 5) + 1   -- Checkout: 1-5
      WHEN RAND() <= 0.95 THEN FLOOR(RAND() * 3) + 1   -- Payment: 1-3
      ELSE 1  -- Success: 1
    END AS impressions,
    -- Random clicks (always <= impressions)
    FLOOR(RAND() * 8) + 1 AS clicks,
    -- Device type
    CASE 
      WHEN RAND() <= 0.60 THEN 'Mobile'
      WHEN RAND() <= 0.85 THEN 'Desktop'
      ELSE 'Tablet'
    END AS device_type,
    -- Traffic source
    CASE 
      WHEN RAND() <= 0.30 THEN 'Organic Search'
      WHEN RAND() <= 0.50 THEN 'Direct'
      WHEN RAND() <= 0.70 THEN 'Social Media'
      WHEN RAND() <= 0.85 THEN 'Paid Search'
      ELSE 'Email'
    END AS traffic_source,
    -- Random session duration (in minutes)
    FLOOR(RAND() * 45) + 1 AS session_duration_minutes,
    -- Random page views
    FLOOR(RAND() * 12) + 1 AS page_views
  FROM date_range
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, 1000)) AS row_num  -- 1000 rows per day
  WHERE RAND() < 0.8  -- Randomly filter to get ~800 rows per day
),

enhanced_data AS (
  SELECT 
    *,
    -- Ensure clicks don't exceed impressions
    LEAST(clicks, impressions) AS adjusted_clicks,
    -- Calculate conversion (1 if reached Success stage, 0 otherwise)
    CASE WHEN funnel_stage = 'Success' THEN 1 ELSE 0 END AS conversion,
    -- Calculate bounce (1 if only 1 page view and short session, 0 otherwise)
    CASE WHEN page_views = 1 AND session_duration_minutes <= 2 THEN 1 ELSE 0 END AS bounce,
    -- Calculate return user (random but higher probability for Repeat segment)
    CASE 
      WHEN user_segment = 'Repeat' AND RAND() <= 0.8 THEN 1
      WHEN user_segment = 'New' AND RAND() <= 0.1 THEN 1
      WHEN user_segment = 'NTU' AND RAND() <= 0.3 THEN 1
      ELSE 0
    END AS is_return_user,
    -- Calculate revenue (only for successful conversions)
    CASE 
      WHEN funnel_stage = 'Success' THEN 
        CASE vertical
          WHEN 'E-commerce' THEN ROUND(RAND() * 200 + 20, 2)  -- $20-220
          WHEN 'Food Delivery' THEN ROUND(RAND() * 50 + 10, 2) -- $10-60
          WHEN 'Travel' THEN ROUND(RAND() * 1000 + 100, 2)     -- $100-1100
          WHEN 'Finance' THEN ROUND(RAND() * 500 + 50, 2)      -- $50-550
          WHEN 'Entertainment' THEN ROUND(RAND() * 30 + 5, 2)  -- $5-35
        END
      ELSE 0
    END AS revenue,
    -- Add timestamps for more realistic data
    TIMESTAMP_ADD(TIMESTAMP(date), INTERVAL CAST(FLOOR(RAND() * 24) AS INT64) HOUR) AS event_timestamp,
    -- Add geographic data
    CASE 
      WHEN RAND() <= 0.25 THEN 'US'
      WHEN RAND() <= 0.45 THEN 'UK'
      WHEN RAND() <= 0.65 THEN 'Canada'
      WHEN RAND() <= 0.80 THEN 'Australia'
      ELSE 'India'
    END AS country
  FROM base_data
),

final_data AS (
  SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(WEEK FROM date) AS week,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    user_id,
    session_id,
    user_segment,
    platform,
    vertical,
    funnel_stage,
    impressions,
    adjusted_clicks AS clicks,
    conversion,
    bounce,
    is_return_user,
    revenue,
    event_timestamp,
    device_type,
    traffic_source,
    session_duration_minutes,
    page_views,
    country,
    -- Calculate CTR
    SAFE_DIVIDE(adjusted_clicks, impressions) AS click_through_rate,
    -- Calculate conversion rate
    SAFE_DIVIDE(conversion, adjusted_clicks) AS conversion_rate,
    -- Add some calculated fields for dashboard
    CASE WHEN session_duration_minutes >= 10 THEN 1 ELSE 0 END AS engaged_session,
    CASE WHEN page_views >= 3 THEN 1 ELSE 0 END AS deep_engagement,
    -- Add cohort information (month of first visit)
    CASE 
      WHEN user_segment = 'New' THEN FORMAT_DATE('%Y-%m', date)
      ELSE NULL 
    END AS cohort_month
  FROM enhanced_data
)

SELECT * FROM final_data
ORDER BY date, event_timestamp;