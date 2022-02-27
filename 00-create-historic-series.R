source("99-pkgs-funs-dirs.R")

# conversion codes ----

load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")
load("../comtrade-codes/02-2-tidy-product-data/product-correlation.RData")

hs02_to_hs12 <- product_correlation %>%
  select(hs02, hs12) %>%
  arrange(hs12) %>%
  distinct(hs02, .keep_all = T)

# country codes ----

# d_hs <- open_dataset("hs12-historic",
#                        partitioning = c("trade_flow", "year", "reporter_iso"))
#
# codes <- map_df(
#   2002:2020,
#   function(y) {
#     message(y)
#
#     y2 <- paste0("year=", y)
#
#     d1 <- d_hs02 %>%
#       filter(year == y2, aggregate_level == "aggregate_level=0") %>%
#       select(year, reporter_iso, reporter_code, reporter) %>%
#       collect() %>%
#       mutate(
#         year = remove_hive(year),
#         reporter_iso = remove_hive(reporter_iso)
#       ) %>%
#       rename(country_iso = reporter_iso,
#              country_code = reporter_code,
#              country = reporter) %>%
#       distinct()
#
#     d2 <- d_hs02 %>%
#       filter(year == y2, aggregate_level == "aggregate_level=0") %>%
#       select(year, partner_iso, partner_code, partner) %>%
#       collect() %>%
#       mutate(year = remove_hive(year)) %>%
#       rename(country_iso = partner_iso,
#              country_code = partner_code,
#              country = partner) %>%
#       distinct()
#
#     d <- d1 %>% bind_rows(d2) %>% distinct()
#
#     return(d)
#   }
# )

# hs02 ----

d_hs02 <- open_dataset("../uncomtrade-datasets-arrow/hs-rev2002/parquet/",
                       partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

map(
  2002:2014,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-historic/trade_flow=export/", y2))) {
      return(TRUE)
    }

    d6 <- d_hs02 %>%
      filter(
        year == y2,
        aggregate_level == "aggregate_level=6"
      ) %>%
      select(trade_flow, year,
             reporter_iso, partner_iso,
             reporter_code, partner_code,
             commodity_code, trade_value_usd) %>%
      collect() %>%
      filter(!partner_iso %in% c("wld","all")) %>%
      mutate(
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      left_join(hs02_to_hs12, by = c("commodity_code" = "hs02"))

    d6 <- d6 %>%
      mutate(
        reporter_iso = case_when(
          reporter_code == 80 ~ "e-80", # br. antarctic terr.
          reporter_code == 129 ~ "e-129", # Caribbean, not elsewhere specified
          reporter_code == 221 ~ "e-221", # Eastern Europe, not elsewhere specified
          reporter_code == 290 ~ "e-290", # Northern Africa, not elsewhere specified
          reporter_code == 471 ~ "e-471", # Central American Common Market, not elsewhere specified
          reporter_code == 472 ~ "e-472", # Africa CAMEU region, not elsewhere specified
          reporter_code == 473 ~ "e-473", # laia, nes
          reporter_code == 490 ~ "e-490", # other asia, nes
          reporter_code == 492 ~ "e-492", # Europe EU, not elsewhere specified
          reporter_code == 527 ~ "e-527", # oceania, nes
          reporter_code == 536 ~ "e-536", # neutral zone
          reporter_code == 568 ~ "e-568", # other europe, nes
          reporter_code == 577 ~ "e-577", # other africa, nes
          reporter_code == 636 ~ "e-636", # Rest of America, not elsewhere specified
          reporter_code == 637 ~ "e-637", # north america and central america, nes
          reporter_code == 697 ~ "e-697", # Europe EFTA, not elsewhere specified
          reporter_code == 837 ~ "e-837", # bunkers
          reporter_code == 838 ~ "e-838", # free zones
          reporter_code == 839 ~ "e-839", # special categories
          reporter_code == 849 ~ "e-849", # us misc. pacific isds
          reporter_code == 879 ~ "e-879", # Western Asia, not elsewhere specified
          reporter_code == 899 ~ "e-899", # areas, nes
          TRUE ~ reporter_iso
        ),
        partner_iso = case_when(
          partner_code == 80 ~ "e-80", # br. antarctic terr.
          partner_code == 129 ~ "e-129", # Caribbean, not elsewhere specified
          partner_code == 221 ~ "e-221", # Eastern Europe, not elsewhere specified
          partner_code == 290 ~ "e-290", # Northern Africa, not elsewhere specified
          partner_code == 471 ~ "e-471", # Central American Common Market, not elsewhere specified
          partner_code == 472 ~ "e-472", # Africa CAMEU region, not elsewhere specified
          partner_code == 473 ~ "e-473", # laia, nes
          partner_code == 490 ~ "e-490", # other asia, nes
          partner_code == 492 ~ "e-492", # Europe EU, not elsewhere specified
          partner_code == 527 ~ "e-527", # oceania, nes
          partner_code == 536 ~ "e-536", # neutral zone
          partner_code == 568 ~ "e-568", # other europe, nes
          partner_code == 577 ~ "e-577", # other africa, nes
          partner_code == 636 ~ "e-636", # Rest of America, not elsewhere specified
          partner_code == 637 ~ "e-637", # north america and central america, nes
          partner_code == 697 ~ "e-697", # Europe EFTA, not elsewhere specified
          partner_code == 837 ~ "e-837", # bunkers
          partner_code == 838 ~ "e-838", # free zones
          partner_code == 839 ~ "e-839", # special categories
          partner_code == 849 ~ "e-849", # us misc. pacific isds
          partner_code == 879 ~ "e-879", # Western Asia, not elsewhere specified
          partner_code == 899 ~ "e-899", # areas, nes
          TRUE ~ partner_iso
        )
      )

    d6_check1 <- d6 %>%
      ungroup() %>%
      select(country_iso = reporter_iso) %>%
      distinct()

    d6_check2 <- d6 %>%
      ungroup() %>%
      select(country_iso = partner_iso) %>%
      distinct()

    d6_check <- d6_check1 %>% bind_rows(d6_check2) %>% distinct()

    d6_check <- d6_check %>%
      filter(nchar(country_iso) > 5) %>% distinct()

    print(d6_check)

    stopifnot(nrow(d6_check) == 0)

    d6 <- d6 %>%
      select(trade_flow, year, reporter_iso, partner_iso, commodity_code = hs12, trade_value_usd) %>%
      mutate(
        commodity_code = case_when(
          is.na(commodity_code) ~ "999999",
          TRUE ~ commodity_code
        )
      ) %>%
      group_by(trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T))

    gc()

    missing_codes_count <- d6 %>%
      ungroup() %>%
      filter(is.na(commodity_code)) %>%
      count() %>%
      pull()

    stopifnot(missing_codes_count == 0)

    d6 %>%
      group_by(trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    rm(d6); gc()
  }
)

# hs12 ----

d_hs12 <- open_dataset("../uncomtrade-datasets-arrow/hs-rev2012/parquet/",
                     partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

map(
  2015:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-historic/trade_flow=export/", y2))) {
      return(TRUE)
    }

    d6 <- d_hs12 %>%
      filter(
        year == y2,
        aggregate_level == "aggregate_level=6",
      ) %>%
      select(trade_flow, year,
             reporter_iso, partner_iso,
             reporter_code, partner_code,
             commodity_code, trade_value_usd) %>%
      collect() %>%
      filter(!partner_iso %in% c("wld","all")) %>%
      mutate(
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      )

    d6 <- d6 %>%
      mutate(
        reporter_iso = case_when(
          reporter_code == 80 ~ "e-80", # br. antarctic terr.
          reporter_code == 129 ~ "e-129", # Caribbean, not elsewhere specified
          reporter_code == 221 ~ "e-221", # Eastern Europe, not elsewhere specified
          reporter_code == 290 ~ "e-290", # Northern Africa, not elsewhere specified
          reporter_code == 471 ~ "e-471", # Central American Common Market, not elsewhere specified
          reporter_code == 472 ~ "e-472", # Africa CAMEU region, not elsewhere specified
          reporter_code == 473 ~ "e-473", # laia, nes
          reporter_code == 490 ~ "e-490", # other asia, nes
          reporter_code == 492 ~ "e-492", # Europe EU, not elsewhere specified
          reporter_code == 527 ~ "e-527", # oceania, nes
          reporter_code == 536 ~ "e-536", # neutral zone
          reporter_code == 568 ~ "e-568", # other europe, nes
          reporter_code == 577 ~ "e-577", # other africa, nes
          reporter_code == 636 ~ "e-636", # Rest of America, not elsewhere specified
          reporter_code == 637 ~ "e-637", # north america and central america, nes
          reporter_code == 697 ~ "e-697", # Europe EFTA, not elsewhere specified
          reporter_code == 837 ~ "e-837", # bunkers
          reporter_code == 838 ~ "e-838", # free zones
          reporter_code == 839 ~ "e-839", # special categories
          reporter_code == 849 ~ "e-849", # us misc. pacific isds
          reporter_code == 879 ~ "e-879", # Western Asia, not elsewhere specified
          reporter_code == 899 ~ "e-899", # areas, nes
          TRUE ~ reporter_iso
        ),
        partner_iso = case_when(
          partner_code == 80 ~ "e-80", # br. antarctic terr.
          partner_code == 129 ~ "e-129", # Caribbean, not elsewhere specified
          partner_code == 221 ~ "e-221", # Eastern Europe, not elsewhere specified
          partner_code == 290 ~ "e-290", # Northern Africa, not elsewhere specified
          partner_code == 471 ~ "e-471", # Central American Common Market, not elsewhere specified
          partner_code == 472 ~ "e-472", # Africa CAMEU region, not elsewhere specified
          partner_code == 473 ~ "e-473", # laia, nes
          partner_code == 490 ~ "e-490", # other asia, nes
          partner_code == 492 ~ "e-492", # Europe EU, not elsewhere specified
          partner_code == 527 ~ "e-527", # oceania, nes
          partner_code == 536 ~ "e-536", # neutral zone
          partner_code == 568 ~ "e-568", # other europe, nes
          partner_code == 577 ~ "e-577", # other africa, nes
          partner_code == 636 ~ "e-636", # Rest of America, not elsewhere specified
          partner_code == 637 ~ "e-637", # north america and central america, nes
          partner_code == 697 ~ "e-697", # Europe EFTA, not elsewhere specified
          partner_code == 837 ~ "e-837", # bunkers
          partner_code == 838 ~ "e-838", # free zones
          partner_code == 839 ~ "e-839", # special categories
          partner_code == 849 ~ "e-849", # us misc. pacific isds
          partner_code == 879 ~ "e-879", # Western Asia, not elsewhere specified
          partner_code == 899 ~ "e-899", # areas, nes
          TRUE ~ partner_iso
        )
      )

    d6_check1 <- d6 %>%
      ungroup() %>%
      select(country_iso = reporter_iso) %>%
      distinct()

    d6_check2 <- d6 %>%
      ungroup() %>%
      select(country_iso = partner_iso) %>%
      distinct()

    d6_check <- d6_check1 %>% bind_rows(d6_check2) %>% distinct()

    d6_check <- d6_check %>%
      filter(nchar(country_iso) > 5) %>% distinct()

    print(d6_check)

    stopifnot(nrow(d6_check) == 0)

    d6 <- d6 %>%
      group_by(trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T))

    gc()

    d6 %>%
      group_by(trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    rm(d6); gc()
  }
)
