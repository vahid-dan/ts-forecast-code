d <- read_csv("/Users/quinn/Dropbox (VTFRS)/Research/SSC_forecasting/run_flare_package/test_aed/test_glm_aed_sss_H_2018_07_20_2019_12_31_F_0_20210601T092954.csv")


d %>% group_by(state, depth) %>%
  summarize(sd = sd(forecast_mean - observed)) %>%
  ggplot(aes(x = depth, y = sd)) +
  geom_line() +
  facet_wrap(vars(state), scale = "free")

d1 <- d %>% group_by(state, depth) %>%
  summarize(sd = sd(forecast_mean - observed)) %>%
  pivot_wider(names_from = state, values_from = sd) %>%
  arrange(depth)

d1$NIT_amm <- approx(y = d1$NIT_amm, x = d1$depth,xout = d1$depth)$y
d1$NIT_nit <- approx(y = d1$NIT_nit, x = d1$depth,xout = d1$depth)$y
d1$NIT_total <- approx(y = d1$NIT_total, x = d1$depth,xout = d1$depth)$y
d1$OGM_doc_total <- approx(y = d1$OGM_doc_total, x = d1$depth,xout = d1$depth)$y
d1$OXY_oxy <- approx(y = d1$OXY_oxy, x = d1$depth,xout = d1$depth)$y
d1$PHS_frp <- approx(y = d1$PHS_frp, x = d1$depth,xout = d1$depth)$y
d1$PHS_total <- approx(y = d1$PHS_total, x = d1$depth,xout = d1$depth)$y
d1$PHY_TCHLA <- approx(y = d1$PHY_TCHLA,x =  d1$depth,xout = d1$depth)$y
d1$temp <- approx(y= d1$temp, x = d1$depth,xout = d1$depth)$y

  write_csv(d1, file = "/Users/quinn/Dropbox (VTFRS)/Research/SSC_forecasting/run_flare_package/test_aed/state_error.csv")
