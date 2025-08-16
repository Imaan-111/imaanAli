library(DBI)
library(RSQLite)

con <- dbConnect(RSQLite::SQLite(), "/home/imaan4120/project1_rfq.db")
dbListTables(con)

# -----------------------------
# Step 1: Find top 3 products by total units sold
# -----------------------------

top_products <- dbGetQuery(con, "
  SELECT item_nbr, SUM(units) AS total_units
  FROM train
  GROUP BY item_nbr
  ORDER BY total_units DESC
  LIMIT 3;
")

top_products

# -----------------------------
# Step 2: Join train table with key table
# -----------------------------

sales_with_station <- dbGetQuery(con, "
  SELECT t.*, k.station_nbr
  FROM train t
  JOIN key k
    ON t.store_nbr = k.store_nbr;
")

head(sales_with_station)

# -----------------------------
# Step 3: Query daily sales and average temperature
# -----------------------------

top_item <- top_products$item_nbr[1]

daily_sales_weather <- dbGetQuery(con, paste0("
  SELECT t.date, SUM(t.units) AS daily_units, AVG(w.tavg) AS avg_temp
  FROM train t
  JOIN key k
    ON t.store_nbr = k.store_nbr
  JOIN weather w
    ON k.station_nbr = w.station_nbr
    AND t.date = w.date
  WHERE t.item_nbr = ", top_item, "
  GROUP BY t.date
  ORDER BY t.date;
"))

head(daily_sales_weather)


# -----------------------------
# Step 4: Create a joined dataset for top 3 products with weather
# -----------------------------

# Get top 3 product numbers
top_items <- paste(top_products$item_nbr, collapse = ",")

# Join sales_with_station with weather table and filter top 3 products
sales_top3 <- dbGetQuery(con, paste0("
  SELECT s.*, w.tavg, w.snow, w.precip
  FROM sales_with_station s
  JOIN weather w
    ON s.station_nbr = w.station_nbr
   AND s.date = w.date
  WHERE s.item_nbr IN (", top_items, ")
  ORDER BY s.item_nbr, s.date;
"))


# -----------------------------
# Step 4: Create a joined dataset for top 3 products with weather
# -----------------------------

dbListFields(con, "weather")


# Get top 3 product numbers
top_items <- paste(top_products$item_nbr, collapse = ",")

dbWriteTable(con, "sales_with_station", sales_with_station, overwrite = TRUE)

# Join sales_with_station with weather table and filter top 3 products
sales_top3 <- dbGetQuery(con, paste0("
  SELECT s.*, w.tavg, w.snowfall, w.preciptotal, w.heat, w.cool
  FROM sales_with_station s
  JOIN weather w
    ON s.station_nbr = w.station_nbr
   AND s.date = w.date
  WHERE s.item_nbr IN (", top_items, ")
  ORDER BY s.item_nbr, s.date;
"))


# Preview the dataset
head(sales_top3)

# Save as RDS for Stage 2 analysis
saveRDS(sales_top3, "sales_top3.rds")
