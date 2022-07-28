library(tidyverse)
library(lubridate)
set.seed(100)

Sys.setenv("AWS_DEFAULT_REGION" = "s3",
           "AWS_S3_ENDPOINT" = "flare-forecast.org")

lake_directory <- here::here()

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

sim_names <- "ms_glmead_flare"

config_files <- paste0("configure_flare_aed_run.yml")

#num_forecasts <- 20
num_forecasts <- 19 * 7 + 1
days_between_forecasts <- 7
forecast_horizon <- 34 #32
starting_date <- as_date("2018-07-20")
second_date <- as_date("2019-01-01") - days(days_between_forecasts)

start_dates <- rep(NA, num_forecasts)
start_dates[1:2] <- c(starting_date, second_date)
for(i in 3:num_forecasts){
  start_dates[i] <- as_date(start_dates[i-1]) + days(days_between_forecasts)
}

start_dates <- as_date(start_dates)
forecast_start_dates <- start_dates + days(days_between_forecasts)
forecast_start_dates <- forecast_start_dates[-1]

configure_run_file <- "configure_flare_aed.yml"

for(j in 1:length(sites)){

  #function(i, sites, lake_directory, sim_names, config_files, )

  message(paste0("Running site: ", sites[j]))

  run_config <- yaml::read_yaml(file.path(lake_directory, "configuration", "FLAREr", configure_run_file))
  run_config$configure_flare <- config_files[j]
  run_config$sim_name <- sim_names
  yaml::write_yaml(run_config, file = file.path(lake_directory, "configuration", "FLAREr", configure_run_file))

  if(file.exists(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))){
    unlink(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))
  }

  ##'
  # Set up configurations for the data processing
  config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml")
  config <- FLAREr::set_configuration(configure_run_file,lake_directory)

  use_s3 <- FALSE

  #' Clone or pull from data repositories

  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_insitu_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_met_station_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  FLAREr::get_git_repo(lake_directory,
                       directory = config_obs$realtime_inflow_data_location,
                       git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  #get_git_repo(lake_directory,
  #             directory = config_obs$manual_data_location,
  #             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

  #' Download files from EDI

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/389/5/3d1866fecfb8e17dc902c76436239431",
                       file = config_obs$met_raw_obs_fname[2],
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/5/c1b1f16b8e3edbbff15444824b65fe8f",
                       file = config_obs$insitu_obs_fname[2],
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/198/8/336d0a27c4ae396a75f4c07c01652985",
                       file = config_obs$secchi_fname,
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/200/11/d771f5e9956304424c3bc0a39298a5ce",
                       file = config_obs$ctd_fname,
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/199/8/da174082a3d924e989d3151924f9ef98",
                       file = config_obs$nutrients_fname,
                       lake_directory)


  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/202/7/f5fa5de4b49bae8373f6e7c1773b026e",
                       file = config_obs$inflow_raw_file1[2],
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95",
                       file = "silica_master_df.csv",
                       lake_directory)

  FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/551/5/38d72673295864956cccd6bbba99a1a3",
                       file = "Dissolved_CO2_CH4_Virginia_Reservoirs.csv",
                       lake_directory)

  #' Clean up observed meterology

  cleaned_met_file <- met_qaqc(realtime_file = file.path(config_obs$file_path$data_directory, config_obs$met_raw_obs_fname[1]),
                               qaqc_file = file.path(config_obs$file_path$data_directory, config_obs$met_raw_obs_fname[2]),
                               cleaned_met_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id,paste0("observed-met_",config_obs$site_id,".nc")),
                               input_file_tz = "EST",
                               nldas = NULL)

  #' Clean up observed inflow

  cleaned_inflow_file <- inflow_qaqc(realtime_file = file.path(config_obs$file_path$data_directory, config_obs$inflow_raw_file1[1]),
                                     qaqc_file = file.path(config_obs$file_path$data_directory, config_obs$inflow_raw_file1[2]),
                                     nutrients_file = file.path(config_obs$file_path$data_directory, config_obs$nutrients_fname),
                                     silica_file = file.path(config_obs$file_path$data_directory,  config_obs$silica_fname),
                                     co2_ch4 = file.path(config_obs$file_path$data_directory, config_obs$ch4_fname),
                                     cleaned_inflow_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-inflow.csv")),
                                     input_file_tz = 'EST')

  #' Clean up observed insitu measurements

  cleaned_insitu_file <- in_situ_qaqc(insitu_obs_fname = file.path(config_obs$file_path$data_directory,config_obs$insitu_obs_fname),
                                      data_location = config_obs$file_path$data_directory,
                                      maintenance_file = file.path(config_obs$file_path$data_directory,config_obs$maintenance_file),
                                      ctd_fname = file.path(config_obs$file_path$data_directory, config_obs$ctd_fname),
                                      nutrients_fname =  file.path(config_obs$file_path$data_directory, config_obs$nutrients_fname),
                                      secchi_fname = file.path(config_obs$file_path$data_directory, config_obs$secchi_fname),
                                      ch4_fname = file.path(config_obs$file_path$data_directory, config_obs$ch4_fname),
                                      cleaned_insitu_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-insitu.csv")),
                                      lake_name_code = config_obs$site_id,
                                      config = config_obs)

  ##` Download NOAA forecasts`

  message("    Downloading NOAA data")

  cycle <- "00"

  for(i in 1:length(forecast_start_dates)){
    noaa_forecast_path <- file.path(config$met$forecast_met_model, config$location$site_id, forecast_start_dates[i], "00")
    FLAREr::get_driver_forecast(lake_directory, forecast_path = noaa_forecast_path)
  }

  config$run_config$start_datetime <- as.character(paste0(start_dates[1], " 00:00:00"))
  config$run_config$forecast_start_datetime <- as.character(paste0(start_dates[2], " 00:00:00"))
  config$run_config$forecast_horizon <- 0
  config$run_config$restart_file <- NA
  run_config <- config$run_config
  yaml::write_yaml(run_config, file = file.path(config$file_path$configuration_directory, "FLAREr", configure_run_file))

  for(i in 1:length(forecast_start_dates)){

    config <- FLAREr::set_configuration(configure_run_file,lake_directory)
    config <- FLAREr::get_restart_file(config, lake_directory)

    message(paste0("     Running forecast that starts on: ", config$run_config$start_datetime))

    if(config$run_config$forecast_horizon > 0){
      noaa_forecast_path <- FLAREr::get_driver_forecast_path(config,
                                                             forecast_model = config$met$forecast_met_model)
      forecast_dir <- file.path(config$file_path$noaa_directory, noaa_forecast_path)
    }else{
      forecast_dir <- NULL
    }

    config$future_inflow_flow_coeff <- c(0.0010803, 0.9478724, 0.3478991)
    config$future_inflow_flow_error <- 0.00965
    config$future_inflow_temp_coeff <- c(0.20291, 0.94214, 0.04278)
    config$future_inflow_temp_error <- 0.943

    forecast_files <- list.files(file.path(lake_directory, "drivers", noaa_forecast_path), full.names = TRUE)
    if(length(forecast_files) == 0){
      stop(paste0("missing forecast files at: ", noaa_forecast_path))
    }
    temp_flow_forecast <- forecast_inflows_outflows(inflow_obs = file.path(config$file_path$qaqc_data_directory, "fcre-targets-inflow.csv"),
                                                    forecast_files = forecast_files,
                                                    obs_met_file = file.path(config$file_path$qaqc_data_directory,"observed-met_fcre.nc"),
                                                    output_dir = config$file_path$inflow_directory,
                                                    inflow_model = config$inflow$forecast_inflow_model,
                                                    inflow_process_uncertainty = FALSE,
                                                    forecast_location = config$file_path$forecast_output_directory,
                                                    config = config,
                                                    use_s3 = config$run_config$use_s3,
                                                    bucket = "drivers",
                                                    model_name = config$model_settings$model_name)


    #Need to remove the 00 ensemble member because it only goes 16-days in the future
    met_out$filenames <- met_out$filenames[!stringr::str_detect(met_out$filenames, "ens00")]

    pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
    obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$obs_config_file), col_types = readr::cols())
    states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$states_config_file), col_types = readr::cols())


    #Download and process observations (already done)

    met_out <- FLAREr::generate_glm_met_files(obs_met_file = file.path(config$file_path$qaqc_data_directory, paste0("observed-met_",config$location$site_id,".nc")),
                                              out_dir = config$file_path$execute_directory,
                                              forecast_dir = forecast_dir,
                                              config = config)


    inflow_outflow_files <- FLAREr::create_glm_inflow_outflow_files(inflow_file_dir = inflow_file_dir,
                                                                    inflow_obs = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-inflow.csv")),
                                                                    working_directory = config$file_path$execute_directory,
                                                                    config = config,
                                                                    state_names = states_config$state_names)

    management <- NULL

    if(config$model_settings$model_name == "glm_aed"){

      https_file <- "https://raw.githubusercontent.com/cayelan/FCR-GLM-AED-Forecasting/master/FCR_2013_2019GLMHistoricalRun_GLMv3beta/inputs/FCR_SSS_inflow_2013_2021_20211102_allfractions_2DOCpools.csv"
      download.file(https_file,
                    file.path(config$file_path$execute_directory, basename(https_file)))

      inflow_outflow_files$inflow_file_name <- cbind(inflow_outflow_files$inflow_file_name, rep(file.path(config$file_path$execute_directory,basename(https_file)), length(inflow_outflow_files$inflow_file_name)))
    }

    #Create observation matrix
    obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                     obs_config = obs_config,
                                     config)

    states_config <- FLAREr::generate_states_to_obs_mapping(states_config, obs_config)

    model_sd <- FLAREr::initiate_model_error(config, states_config)

    init <- FLAREr::generate_initial_conditions(states_config,
                                                obs_config,
                                                pars_config,
                                                obs,
                                                config,
                                                restart_file = config$run_config$restart_file,
                                                historical_met_error = met_out$historical_met_error)
    #Run EnKF
    da_forecast_output <- FLAREr::run_da_forecast(states_init = init$states,
                                                  pars_init = init$pars,
                                                  aux_states_init = init$aux_states_init,
                                                  obs = obs,
                                                  obs_sd = obs_config$obs_sd,
                                                  model_sd = model_sd,
                                                  working_directory = config$file_path$execute_directory,
                                                  met_file_names = met_out$filenames,
                                                  inflow_file_names = inflow_outflow_files$inflow_file_name,
                                                  outflow_file_names = inflow_outflow_files$outflow_file_name,
                                                  config = config,
                                                  pars_config = pars_config,
                                                  states_config = states_config,
                                                  obs_config = obs_config,
                                                  management,
                                                  da_method = config$da_setup$da_method,
                                                  par_fit_method = config$da_setup$par_fit_method)

    # Save forecast

    saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                                forecast_output_directory = config$file_path$forecast_output_directory,
                                                use_short_filename = TRUE)

    #Create EML Metadata
    eml_file_name <- FLAREr::create_flare_metadata(file_name = saved_file,
                                                   da_forecast_output = da_forecast_output)

    rm(da_forecast_output)
    gc()
    message("Generating plot")
    FLAREr::plotting_general_2(file_name = saved_file,
                               target_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")),
                               ncore = 2,
                               obs_csv = FALSE)

    FLAREr::put_forecast(saved_file, eml_file_name, config)

    FLAREr::update_run_config(config, lake_directory, configure_run_file, saved_file, new_horizon = forecast_horizon, day_advance = days_between_forecasts)

    if(i > 1){

      message("   creating climatology forecast")

      forecast_dates <- seq(as_date(config$run_config$forecast_start_datetime) + days(1),
                            as_date(config$run_config$forecast_start_datetime) + days(forecast_horizon), "1 day")
      forecast_doy <- yday(forecast_dates)

      #curr_month <- month(Sys.Date())
      curr_month <- month(config$run_config$forecast_start_datetime)
      if(curr_month < 10){
        curr_month <- paste0("0", curr_month)
      }
      #curr_year <- year(Sys.Date())
      curr_year <- year(config$run_config$forecast_start_datetime)
      start_date <- as_date(paste(curr_year,curr_month, "01", sep = "-"))

      target <- read_csv(cleaned_observations_file_long, show_col_types = FALSE) %>%
        filter(hour == 0) %>%
        group_by(date, hour, depth, variable) %>%
        summarize(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
        select(date, hour, depth, value, variable)

      target_clim <- target %>%
        filter(date < as_date(config$run_config$forecast_start_datetime),
               variable == "temperature") %>%
        mutate(doy = yday(date)) %>%
        group_by(doy, depth) %>%
        summarise(temp_clim = mean(value, na.rm = TRUE),
                  temp_sd = sd(value, na.rm = TRUE), .groups = "drop") %>%
        mutate(temp_sd = mean(temp_sd, na.rm = TRUE)) %>%
        mutate(temp_clim = ifelse((is.nan(temp_clim)), NA, temp_clim)) %>%
        na.omit()

      clim_forecast <- target_clim %>%
        mutate(doy = as.integer(doy)) %>%
        filter(doy %in% forecast_doy) %>%
        mutate(time = as_date((doy-1), origin = paste(year(Sys.Date()), "01", "01", sep = "-"))) %>%
        select(time, depth , temp_clim, temp_sd) %>%
        rename(mean = temp_clim,
               sd = temp_sd) %>%
        pivot_longer(c("mean", "sd"),names_to = "statistic", values_to = "temperature") %>%
        mutate(forecast = ifelse(time >= as_date(config$run_config$forecast_start_datetime), 1, 0)) %>%
        select(time, depth, statistic, forecast, temperature) %>%
        arrange(depth, time, statistic)

      forecast_file <- paste(sites[j], as_date(config$run_config$forecast_start_datetime), "ms_climatology.csv.gz", sep = "-")

      saved_file <- file.path(config$file_path$forecast_output_directory, forecast_file)
      write_csv(clim_forecast, file = saved_file)

      if (config$run_config$use_s3) {
        success <- aws.s3::put_object(file = saved_file, object =         file.path(config$location$site_id,
                                      basename(saved_file)), bucket = "forecasts",
                                      region = Sys.getenv("AWS_DEFAULT_REGION"),
                                      use_https = as.logical(Sys.getenv("USE_HTTPS")))
        if (success) {
          unlink(saved_file)
        }
      }

      message("   creating persistence forecast")

      most_recent_obs <- target %>%
        filter(date < as_date(config$run_config$forecast_start_datetime),
               variable == "temperature") %>%
        na.omit() %>%
        arrange(rev(date))

      ens_size <- config$da_setup$ensemble_size
      walk_sd <- 0.25
      dates <- unique(clim_forecast$time)
      ndates <- length(dates) + 1
      depths <- unique(target$depth)
      random_walk <- array(NA, dim = c(ens_size, ndates, length(depths)))
      persist_forecast <- NULL

      for(k in 1:length(depths)){

        most_recent_obs_depth <- most_recent_obs %>%
          filter(depth == depths[k])

        random_walk[ ,1,k] <- unlist(most_recent_obs_depth$value[1])
        for(m in 2:ndates){
          random_walk[ ,m, k] <- rnorm(ens_size, mean = random_walk[ ,m - 1,k], walk_sd)
        }

        depth_tibble <-  suppressMessages(as_tibble(t(random_walk[,2:ndates ,k]), .name_repair = "unique"))

        names(depth_tibble) <- as.character(seq(1,ens_size, 1))

        depth_tibble <- bind_cols(time = dates,depth_tibble) %>%
          pivot_longer(cols = -time, names_to = "ensemble", values_to = "temperature") %>%
          mutate(forecast = 1,
                 depth = depths[k]) %>%
          select(time, depth, ensemble, forecast, temperature)

        persist_forecast <- bind_rows(persist_forecast, depth_tibble)
      }

      forecast_file <- paste(sites[j], as_date(config$run_config$forecast_start_datetime), "ms_persistence.csv.gz", sep = "-")
      saved_file <- file.path(config$file_path$forecast_output_directory, forecast_file)
      write_csv(persist_forecast, file = file.path(config$file_path$forecast_output_directory, forecast_file))

      if (config$run_config$use_s3) {
        success <- aws.s3::put_object(file = saved_file, object = file.path(config$location$site_id,
                                      basename(saved_file)), bucket = "forecasts",
                                      region = Sys.getenv("AWS_DEFAULT_REGION"),
                                      use_https = as.logical(Sys.getenv("USE_HTTPS")))
        if (success) {
          unlink(saved_file)
        }
      }
    }
  }
}
