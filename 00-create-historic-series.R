source("99-pkgs-funs-dirs.R")

# conversion codes ----

load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")
load("../comtrade-codes/02-2-tidy-product-data/product-correlation.RData")

hs02_to_hs12 <- product_correlation %>%
  select(hs02, hs12) %>%
  arrange(hs12) %>%
  distinct(hs02, .keep_all = T)

# hs12 ----

d_hs12 <- open_dataset("../uncomtrade-datasets-arrow/hs-rev2012/parquet/",
                     partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

# chl2015 <- d_hs12 %>%
#   filter(
#     year == "year=2015",
#     reporter_iso == "reporter_iso=chl",
#     aggregate_level == "aggregate_level=6",
#     partner_iso != "wld"
#   ) %>%
#   select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd) %>%
#   collect() %>%
#   mutate(
#     aggregate_level = remove_hive(aggregate_level),
#     trade_flow = remove_hive(trade_flow),
#     year = remove_hive(year),
#     reporter_iso = remove_hive(reporter_iso)
#   )
#
# sort(unique(chl2015$reporter_iso))
# sort(unique(chl2015$partner_iso))

map(
  2012:2020,
  function(y) {
    message(y)

    y <- paste0("year=", y)

    d6 <- d_hs12 %>%
      filter(
        year == y,
        aggregate_level == "aggregate_level=6",
        partner_iso != "wld"
      ) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd) %>%
      collect() %>%
      mutate(
        aggregate_level = remove_hive(aggregate_level),
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      )

    d4 <- d6 %>%
      mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
      group_by(trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      mutate(aggregate_level = 4) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d6 %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    d4 %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    rm(d6, d4); gc()
  }
)

# hs02 ----

d_hs02 <- open_dataset("../uncomtrade-datasets-arrow/hs-rev2002/parquet/",
  partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

map(
  2002:2011,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d6 <- d_hs02 %>%
      filter(
        year == y2,
        aggregate_level == "aggregate_level=6",
        partner_iso != "wld"
      ) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd) %>%
      collect() %>%
      mutate(
        aggregate_level = remove_hive(aggregate_level),
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      left_join(hs02_to_hs12, by = c("commodity_code" = "hs02"))

    d6 <- d6 %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code = hs12, trade_value_usd) %>%
      mutate(
        commodity_code = case_when(
          is.na(commodity_code) ~ "999999",
          TRUE ~ commodity_code
        )
      ) %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      mutate(aggregate_level = 6) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    missing_codes_count <- d6 %>%
      ungroup() %>%
      filter(is.na(commodity_code)) %>%
      count() %>%
      pull()

    stopifnot(missing_codes_count == 0)

    d4 <- d6 %>%
      mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
      group_by(trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      mutate(aggregate_level = 4) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d6 %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    d4 %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    rm(d6, d4); gc()
  }
)
