library(tidyverse)
library(lubridate)

#diskutil partitionDisk $(hdiutil attach -nomount ram://2048000) 1 GPTFormat APFS 'ramdisk' '100%'

lake_directory <- "/Users/quinn/Downloads/FCRE-forecast-code"
update_run_config <- TRUE #TRUE is used for an iterative workflow


files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

#start_day <- as_date("2018-12-17")
start_day <- as_date("2018-07-20")
forecast_start<- as_date("2018-07-30")
#forecast_start<- as_date("2018-12-31")
#forecast_start<- as_date("2019-12-31")
#forecast_start<- as_date("2019-12-31")
holder1 <- start_day
holder2 <- forecast_start
while(forecast_start < as_date("2020-01-01")){
  start_day <- forecast_start
  forecast_start <- forecast_start + days(7)
  if(forecast_start < as_date("2020-01-01")){
    holder1 <- c(holder1, start_day)
    holder2 <- c(holder2, forecast_start)
  }
}

forecast_days_vector <- rep(16, length(holder1))
forecast_days_vector[1] <- 0
forecasting_timings <- data.frame(holder1,holder2,forecast_days_vector)

saved_file <- NA
#saved_file <- "/Users/quinn/Dropbox/Research/SSC_forecasting/run_flare_package/wrr_runs/wrr_runs_H_2018_07_12_2018_10_17_F_0_20201109T133722.nc"

#for(i in 1:nrow(forecasting_timings)){
for(i in 1:1){

  config_obs <- yaml::read_yaml(file.path(lake_directory,"configuration","observation_processing","observation_processing.yml"))
  config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_flare_glm_aed.yml"))
  run_config <- yaml::read_yaml(config$file_path$run_config)

  run_config$start_datetime <- as.character(forecasting_timings[i,1])
  run_config$forecast_start_datetime <- as.character(forecasting_timings[i, 2])
  run_config$forecast_horizon <- forecasting_timings[i, 3]
  run_config$restart_file <- saved_file
  yaml::write_yaml(run_config, file = config$file_path$run_config)

  met_qaqc(realtime_file = file.path(config_obs$data_location, config_obs$met_raw_obs_fname[1]),
           qaqc_file = file.path(config_obs$data_location, config_obs$met_raw_obs_fname[2]),
           cleaned_met_file_dir = config$file_path$qaqc_data_directory,
           input_file_tz = "EST",
           nldas = file.path(config_obs$data_location, config_obs$nldas))



  cleaned_inflow_file <- paste0(config$file_path$qaqc_data_directory, "/inflow_postQAQC.csv")

  inflow_qaqc(realtime_file = file.path(config_obs$data_location, config_obs$inflow_raw_file1[1]),
              qaqc_file = file.path(config_obs$data_location, config_obs$inflow_raw_file1[2]),
              nutrients_file = file.path(config_obs$data_location, config_obs$nutrients_fname),
              cleaned_inflow_file ,
              input_file_tz = 'EST')


  cleaned_observations_file_long <- paste0(config$file_path$qaqc_data_directory,
                                           "/observations_postQAQC_long.csv")

  in_situ_qaqc(insitu_obs_fname = file.path(config_obs$data_location,config_obs$insitu_obs_fname),
               data_location = config_obs$data_location,
               maintenance_file = file.path(config_obs$data_location,config_obs$maintenance_file),
               ctd_fname = file.path(config_obs$data_location,config_obs$ctd_fname),
               nutrients_fname =  file.path(config_obs$data_location, config_obs$nutrients_fname),
               secchi_fname = file.path(config_obs$data_location, config_obs$secchi_fname),
               cleaned_observations_file_long = cleaned_observations_file_long,
               lake_name_code = config$location$lake_name_code,
               config = config_obs)

  file.copy(file.path(config_obs$data_location,config$management$sss_fname), file.path(config$file_path$qaqc_data_directory,basename(config$management$sss_fname)))

  config$run_config <- run_config

  if(!dir.exists(config$file_path$execute_directory)){
    dir.create(config$file_path$execute_directory)
  }

  pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
  obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$obs_config_file), col_types = readr::cols())
  states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$states_config_file), col_types = readr::cols())


  #Download and process observations (already done)

  cleaned_observations_file_long <- file.path(config$file_path$qaqc_data_directory,"observations_postQAQC_long.csv")
  cleaned_inflow_file <- file.path(config$file_path$qaqc_data_directory, "/inflow_postQAQC.csv")
  observed_met_file <- file.path(config$file_path$qaqc_data_directory,"observed-met_fcre.nc")


  #Step up Drivers

  start_datetime <- lubridate::as_datetime(config$run_config$start_datetime)
  if(is.na(config$run_config$forecast_start_datetime)){
    end_datetime <- lubridate::as_datetime(config$run_config$end_datetime)
    forecast_start_datetime <- end_datetime
  }else{
    forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime)
    end_datetime <- forecast_start_datetime + lubridate::days(config$run_config$forecast_horizon)
  }
  forecast_hour <- lubridate::hour(forecast_start_datetime)
  if(forecast_hour < 10){forecast_hour <- paste0("0",forecast_hour)}
  noaa_forecast_path <- file.path(config$file_path$noaa_directory, config$met$forecast_met_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour)


  forecast_files <- list.files(noaa_forecast_path, full.names = TRUE)


  print("Creating FLARE Met files")
  met_out <- FLAREr::generate_glm_met_files(obs_met_file = observed_met_file,
                                            out_dir = config$file_path$execute_directory,
                                            forecast_dir = noaa_forecast_path,
                                            config = config)

  met_file_names <- met_out$met_file_names
  historical_met_error <- met_out$historical_met_error

  if(config$model_settings$model_name == "glm_aed"){

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_weir_inflow_2013_2019_20200828_allfractions_2poolsDOC.csv"),
              file.path(config$file_path$execute_directory, "FCR_weir_inflow_2013_2019_20200624_allfractions_2poolsDOC.csv"))

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_wetland_inflow_2013_2019_20200828_allfractions_2DOCpools.csv"),
              file.path(config$file_path$execute_directory, "FCR_wetland_inflow_2013_2019_20200713_allfractions_2DOCpools.csv"))

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_SSS_inflow_2013_2019_20200701_allfractions_2DOCpools.csv"),
              file.path(config$file_path$execute_directory, "FCR_SSS_inflow_2013_2019_20200701_allfractions_2DOCpools.csv"))

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_spillway_outflow_SUMMED_WeirWetland_2013_2019_20200615.csv"),
              file.path(config$file_path$execute_directory, "FCR_spillway_outflow_SUMMED_WeirWetland_2013_2019_20200615.csv"))
  }

  print("Creating FLARE Inflow files")

  inflow_forecast_path <- file.path(config$file_path$inflow_directory, config$inflow$forecast_inflow_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour)

  inflow_outflow_files <- FLAREr::create_glm_inflow_outflow_files(inflow_file_dir = inflow_forecast_path,
                                                                  inflow_obs = cleaned_inflow_file,
                                                                  working_directory = config$file_path$execute_directory,
                                                                  config = config,
                                                                  state_names = states_config$state_names)
  #inflow_file_names <- inflow_outflow_files$inflow_file_name
  #outflow_file_names <- inflow_outflow_files$outflow_file_name

  if(config$model_settings$model_name == "glm_aed"){

    file1 <- file.path(config$file_path$execute_directory, "FCR_weir_inflow_2013_2019_20200624_allfractions_2poolsDOC.csv")
    file2 <- file.path(config$file_path$execute_directory, "FCR_wetland_inflow_2013_2019_20200713_allfractions_2DOCpools.csv")
    inflow_file_names <- tibble(file1 = file1,
                                file2 = file2,
                                file3 = "sss_inflow.csv")
    outflow_file_names <- tibble(file_1 = file.path(config$file_path$execute_directory, "FCR_spillway_outflow_SUMMED_WeirWetland_2013_2019_20200615.csv"),
                                 file_2 = "sss_outflow.csv")

    management <- FLAREr::generate_oxygen_management(config = config)
  }else{
    management <- NULL
  }

  #Create observation matrix
  print("Creating Observation Matrix")
  obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long,
                                   obs_config,
                                   config)

  #full_time_forecast <- seq(start_datetime, end_datetime, by = "1 day")
  #obs[,2:dim(obs)[2], ] <- NA

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
  print("Starting EnKF")
  print("-----------------------------------")
  da_forecast_output <- FLAREr::run_da_forecast(states_init = init$states,
                                                pars_init = init$pars,
                                                aux_states_init = init$aux_states_init,
                                                obs = obs,
                                                obs_sd = obs_config$obs_sd,
                                                model_sd = model_sd,
                                                working_directory = config$file_path$execute_directory,
                                                met_file_names = met_out$filenames,
                                                inflow_file_names = inflow_file_names,
                                                outflow_file_names = outflow_file_names,
                                                config = config,
                                                pars_config = pars_config,
                                                states_config = states_config,
                                                obs_config = obs_config,
                                                management,
                                                da_method = config$da_setup$da_method,
                                                par_fit_method = config$da_setup$par_fit_method)

  print("Writing output file")
  saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                              forecast_location = config$file_path$forecast_output_directory)

  #Create EML Metadata
  print("Creating metadata")
  FLAREr::create_flare_metadata(file_name = saved_file,
                                da_forecast_output = da_forecast_output)

  unlist(config$file_path$execute_directory, recursive = TRUE)

  rm(da_forecast_output)
  gc()

  #file_name <- saved_file
  print("Generating plot")
  FLAREr::plotting_general(file_name = saved_file,
                           qaqc_data_directory = config$file_path$qaqc_data_directory,
                           ncore = config$model_settings$ncore,
                           plot_profile = TRUE)

}


