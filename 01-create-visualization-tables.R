source("99-pkgs-funs-dirs.R")

d_hs <- open_dataset("hs12-historic",
  partitioning = c("trade_flow", "year", "reporter_iso"))

# YRPC ------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-visualization/yrpc/", y2))) {
      return(TRUE)
    }

    # exp ----

    d_exp <- d_hs %>%
      filter(
        trade_flow == "trade_flow=export",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_reexp <- d_hs %>%
      filter(
        trade_flow == "trade_flow=re-export",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_exp_corrected <- d_exp %>%
      left_join(d_reexp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      mutate_if(is.numeric, na_to_0) %>%
      rowwise() %>%
      mutate(
        trade_value_usd = max(trade_value_usd.x - trade_value_usd.y, 0)
      ) %>%
      ungroup() %>%
      select(-c(trade_value_usd.x, trade_value_usd.y))

    # imp ----

    d_imp <- d_hs %>%
      filter(
        trade_flow == "trade_flow=import",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_reimp <- d_hs %>%
      filter(
        trade_flow == "trade_flow=re-import",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_imp_corrected <- d_imp %>%
      left_join(d_reimp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      mutate_if(is.numeric, na_to_0) %>%
      rowwise() %>%
      mutate(
        trade_value_usd = max(trade_value_usd.x - trade_value_usd.y, 0)
      ) %>%
      ungroup() %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)

    rm(d_exp, d_reexp, d_imp, d_reimp); gc()

    # full ----

    d_exp_corrected %>%
      select(-trade_flow) %>%
      full_join(d_imp_corrected, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      rename(
        trade_value_usd_exp = trade_value_usd.x,
        trade_value_usd_imp = trade_value_usd.y
      ) %>%
      mutate(year = y) %>%
      select(year, everything()) %>%
      mutate(
        trade_value_usd_exp = na_to_0(trade_value_usd_exp),
        trade_value_usd_imp = na_to_0(trade_value_usd_imp),
        trade_value_usd_exc = trade_value_usd_exp + trade_value_usd_imp
      ) %>%
      filter(trade_value_usd_exc > 0) %>%
      select(-trade_value_usd_exc) %>%
      group_by(year, reporter_iso) %>%
      write_dataset("hs12-visualization/yrpc", hive_style = T)

    rm(d_exp_corrected, d_imp_corrected); gc()
  }
)

# YRP ------------------------------------------------------------------

d_yrpc <- open_dataset("hs12-visualization/yrpc",
                       partitioning = c("year","reporter_iso"))

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-visualization/yrp/", y2))) {
      return(TRUE)
    }

    d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      write_dataset("hs12-visualization/yrp", hive_style = T)
  }
)

# YRC ------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-visualization/yrc/", y2))) {
      return(TRUE)
    }

    d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      group_by(year, reporter_iso, commodity_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year, reporter_iso) %>%
      write_dataset("hs12-visualization/yrc", hive_style = T)
  }
)

# YR -------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-visualization/yr/", y2))) {
      return(TRUE)
    }

    d_yr <- d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      group_by(year, reporter_iso) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      )

    d_yr %>%
      group_by(year) %>%
      write_dataset("hs12-visualization/yr", hive_style = T)
  }
)

# YC -------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-visualization/yc/", y2))) {
      return(TRUE)
    }

    d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
      ) %>%
      group_by(year, commodity_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year) %>%
      write_dataset("hs12-visualization/yc", hive_style = T)
  }
)

# Attributes -------------------------------------------------------------------

load("../comtrade-codes/01-2-tidy-country-data/country-codes.RData")

attributes_countries <- country_codes %>%
  select(
    iso3_digit_alpha, contains("name"), country_abbrevation,
    contains("continent"), eu28_member
  ) %>%
  rename(
    country_iso = iso3_digit_alpha,
    country_abbreviation = country_abbrevation
  ) %>%
  mutate(country_iso = str_to_lower(country_iso)) %>%
  filter(country_iso != "null") %>%
  distinct(country_iso, .keep_all = T) %>%
  select(-country_abbreviation)

load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")

product_names <- product_codes %>%
  filter(
    classification == "H4",
    str_length(code) == 6
  ) %>%
  select(commodity_code = code, commodity_fullname_english = description) %>%
  mutate(group_code = str_sub(commodity_code, 1, 2))

product_names_2 <- product_codes %>%
  filter(
    classification == "H4",
    str_length(code) == 2
  ) %>%
  select(group_code = code, group_name = description)

attributes_commodities <- product_names %>%
  left_join(product_names_2) %>%
  select(commodity_code, commodity_fullname_english,
         group_code, group_fullname_english = group_name)

try(dir.create("hs12-visualization/attributes"))
write_parquet(attributes_countries, "hs12-visualization/attributes/countries.parquet")
write_parquet(attributes_commodities, "hs12-visualization/attributes/commodities.parquet")

# YR-Groups ------------------------------------------------------------------

d_yrc <- open_dataset("hs12-visualization/yrc",
                       partitioning = c("year","reporter_iso"))

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (dir.exists(paste0("hs12-visualization/yr-groups/", y2))) {
      return(TRUE)
    }

    d_yrc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      left_join(attributes_commodities) %>%
      group_by(year, reporter_iso, group_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year) %>%
      write_dataset("hs12-visualization/yr-groups", hive_style = T)
  }
)

# YR-Sections -------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yr-sections/", y2))) {
      open_dataset("hs12-visualization/yrc",
                   partitioning = c("year","reporter_iso")) %>%
        filter(
          year == y2
        ) %>%
        collect() %>%
        mutate(
          year = remove_hive(year),
          reporter_iso = remove_hive(reporter_iso)
        ) %>%
        left_join(
          tradestatistics::ots_commodities %>%
            select(commodity_code, section_code)
        ) %>%
        group_by(year, reporter_iso, section_code) %>%
        summarise(
          trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
          trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
        ) %>%
        group_by(year) %>%
        write_dataset("hs12-visualization/yr-sections", hive_style = T)
    }
  }
)
