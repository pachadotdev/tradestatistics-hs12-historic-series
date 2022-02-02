library(arrow)
library(dplyr)
library(tidyr)
library(purrr)
library(readr)
library(stringr)
library(ggplot2)
library(forcats)

draw <- open_dataset("hs12-visualization/yrpc",
                         partitioning = c("year", "reporter_iso"))

dimputed <- open_dataset("hs12-visualization/yrpc-imputed",
                         partitioning = c("year", "reporter_iso"))

dvenraw <-map_df(
  2002:2020,
  function(y) {
    message(y)

    draw %>%
      filter(year == paste0("year=", y), reporter_iso == "reporter_iso=ven") %>%
      select(year, reporter_iso, trade_value_usd_exp, trade_value_usd_imp) %>%
      collect() %>%
      group_by(year) %>%
      summarise_if(is.numeric, sum)
  }
)

dvenimputed <-map_df(
  2002:2020,
  function(y) {
    message(y)

    dimputed %>%
      filter(year == paste0("year=", y), reporter_iso == "reporter_iso=ven") %>%
      select(year, reporter_iso, trade_value_usd_exp, trade_value_usd_imp) %>%
      collect() %>%
      group_by(year) %>%
      summarise_if(is.numeric, sum)
  }
)

dven <- dvenraw %>%
  mutate(year = as.integer(str_replace_all(year, "[a-z]|=", ""))) %>%
  rename(`Exports, Raw` = trade_value_usd_exp,
         `Imports, Raw` = trade_value_usd_imp) %>%
  gather(group, value, -year) %>%
  bind_rows(
    dvenimputed %>%
      mutate(year = as.integer(str_replace_all(year, "[a-z]|=", ""))) %>%
      rename(`Exports, Imputed` = trade_value_usd_exp,
             `Imports, Imputed` = trade_value_usd_imp) %>%
      gather(group, value, -year)
  )

cols <- c("#93b4f1", "#454548", "#67bb7b", "#eea55e")

ggplot(dven %>% filter(grepl("Raw", group))) +
  geom_line(aes(x = year, y = value, color = group)) +
  geom_point(aes(x = year, y = value, color = group)) +
  expand_limits(x = 2002:2020) +
  labs(title = "Imports and exports of Venezuela 2002-2020, Raw Data") +
  theme_minimal(base_size = 13, base_family = "Roboto Condensed") +
  scale_color_manual(values = cols)

ggplot(dven %>% filter(grepl("Impu", group))) +
  geom_line(aes(x = year, y = value, color = group)) +
  geom_point(aes(x = year, y = value, color = group)) +
  expand_limits(x = 2002:2020) +
  labs(title = "Imports and exports of Venezuela 2002-2020, Imputed Data") +
  theme_minimal(base_size = 13, base_family = "Roboto Condensed") +
  scale_color_manual(values = cols)

rows_with_imputation_per_year <- read_csv("~/github/un_escap/baci-like-hs12/rows_with_imputation_per_year.csv")

rows_with_imputation_per_year <- rows_with_imputation_per_year %>%
  mutate(
    flag2 = case_when(
      flag == "exports, no change" ~ 1,
      flag == "imports, divided by cif/fob ratio" ~ 2,
      flag == "imports, divided by cif/fob ratio due to large exp/imp mismatch" ~ 3,
      flag == "imports, divided by 1 + model constant due to large exp/imp mismatch" ~ 4
    )
  ) %>%
  arrange(year, flag2) %>%
  mutate(flag = as_factor(flag))

rows_with_imputation_per_year %>%
  mutate(
    flag2 = ifelse(flag2 > 1, 2, flag2)
  ) %>%
  group_by(year, flag2) %>%
  summarise(
    m = sum(m)
  ) %>%
  group_by(flag2) %>%
  summarise(
    m = mean(m)
  )

ggplot(rows_with_imputation_per_year) +
  geom_col(aes(x = year, y = n, fill = flag)) +
  theme_minimal(base_size = 13, base_family = "Roboto Condensed") +
  scale_fill_manual(values = cols) +
  theme(legend.position = "bottom", legend.direction="vertical", legend.title = element_blank()) +
  labs(title = "Number of observations by imputation status", y = "No. obs", x = "Year")

ggplot(rows_with_imputation_per_year) +
  geom_col(aes(x = year, y = m, fill = flag)) +
  theme_minimal(base_size = 13, base_family = "Roboto Condensed") +
  scale_fill_manual(values = cols) +
  theme(legend.position = "bottom", legend.direction="vertical", legend.title = element_blank()) +
  labs(title = "Share of observations by imputation status", y = "%. obs", x = "Year")
