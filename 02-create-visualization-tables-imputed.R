source("99-pkgs-funs-dirs.R")

# YRPC ------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yrpc-imputed/", y2))) {
      d <- open_dataset("../baci-like-hs12/imputed_dataset_2002_2020",
                   partitioning = "year") %>%
        filter(year == y2) %>%
        collect() %>%
        mutate(year = remove_hive(year)) %>%
        select(year, reporter_iso, partner_iso, commodity_code, trade_value_usd_exp, trade_value_usd_imp) %>%
        # just avoid decimals
        mutate(
          trade_value_usd_exp = ceiling(trade_value_usd_exp),
          trade_value_usd_imp = ceiling(trade_value_usd_imp)
        )

      d <- d %>%
        arrange(reporter_iso)

      d <- d %>%
        arrange(reporter_iso, partner_iso, commodity_code)

      reporters <- d %>%
        select(reporter_iso) %>%
        distinct() %>%
        pull()

      for (r in reporters) {
        d %>%
          filter(reporter_iso == r) %>%
          group_by(year, reporter_iso) %>%
          write_dataset("hs12-visualization/yrpc-imputed", hive_style = T)
      }

      gc()
    }
  }
)

# open_dataset("hs12-visualization/yrpc-imputed/",
#              partitioning = c("year", "reporter_iso")) %>%
#   filter(year == "year=2002") %>%
#   collect() %>%
#   group_by(reporter_iso) %>%
#   summarise_if(is.numeric, sum, na.rm = T)

# YRP ------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yrp-imputed/", y2))) {
      d <- open_dataset("hs12-visualization/yrpc-imputed",
                        partitioning = c("year","reporter_iso")) %>%
        filter(year == y2) %>%
        collect()

      gc()

      d2 <- d %>%
        mutate(
          year = remove_hive(year),
          reporter_iso = remove_hive(reporter_iso)
        ) %>%
        group_by(year, reporter_iso, partner_iso) %>%
        summarise(
          trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
          trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
        ) %>%
        group_by(year, reporter_iso)

      rm(d); gc()

      d2 %>%
        write_dataset("hs12-visualization/yrp-imputed", hive_style = T)
    }
  }
)

# YRC ------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yrc-imputed/", y2))) {
      d <- open_dataset("hs12-visualization/yrpc-imputed",
                        partitioning = c("year","reporter_iso")) %>%
        filter(
          year == y2
        ) %>%
        collect()

      gc()

      d2 <- d %>%
        mutate(
          year = remove_hive(year),
          reporter_iso = remove_hive(reporter_iso)
        ) %>%
        group_by(year, reporter_iso, commodity_code) %>%
        summarise(
          trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
          trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
        ) %>%
        group_by(year, reporter_iso)

      rm(d); gc()

      d2 %>%
        write_dataset("hs12-visualization/yrc-imputed", hive_style = T)
    }
  }
)

# YR -------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yr-imputed/", y2))) {
      d <- open_dataset("hs12-visualization/yrpc-imputed",
                           partitioning = c("year","reporter_iso")) %>%
        filter(
          year == y2
        ) %>%
        collect()

      gc()

      d2 <- d %>%
        mutate(
          year = remove_hive(year),
          reporter_iso = remove_hive(reporter_iso)
        ) %>%
        group_by(year, reporter_iso) %>%
        summarise(
          trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
          trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
        ) %>%
        group_by(year)

      rm(d); gc()

      d2 %>%
        write_dataset("hs12-visualization/yr-imputed", hive_style = T)
    }
  }
)

# YC -------------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yc-imputed/", y2))) {
      d <- open_dataset("hs12-visualization/yrpc-imputed",
                        partitioning = c("year","reporter_iso")) %>%
        filter(
          year == y2
        ) %>%
        collect()

      gc()

      d2 <- d %>%
        mutate(
          year = remove_hive(year),
        ) %>%
        group_by(year, commodity_code) %>%
        summarise(
          trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
          trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
        ) %>%
        group_by(year)

      rm(d); gc()

      d2 %>%
        write_dataset("hs12-visualization/yc-imputed", hive_style = T)
    }
  }
)

# YR-Groups ------------------------------------------------------------------

attributes_commodities <- read_parquet("hs12-visualization/attributes/commodities.parquet")

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yr-groups-imputed/", y2))) {
      open_dataset("hs12-visualization/yrc-imputed",
                   partitioning = c("year","reporter_iso")) %>%
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
        write_dataset("hs12-visualization/yr-groups-imputed", hive_style = T)
    }
  }
)


# YR-Sections -------------------------------------------------------------

map(
  2002:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    if (!dir.exists(paste0("hs12-visualization/yr-sections-imputed/", y2))) {
      open_dataset("hs12-visualization/yrc-imputed",
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
        write_dataset("hs12-visualization/yr-sections-imputed", hive_style = T)
    }
  }
)
