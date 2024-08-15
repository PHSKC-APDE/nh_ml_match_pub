# Load packages required to define the pipeline:
#targets::tar_visnetwork(T, exclude = c('theform', 'model_version', 'data_version', 'targetfile', 'tdsets', 'tdsets_files','targetfilepath', 'targetfilepath_files'))
# targets::tar_watch(seconds = 120, targets_only = T, exclude = c('theform', 'model_version', 'data_version', 'targetfile', 'tdsets', 'tdsets_files','targetfilepath', 'targetfilepath_files'))
print(Sys.time())
library(targets)
library('tarchetypes')
library('future')
library('future.callr')
library('crew')
tar_option_set(seed = 4)
suppressPackageStartupMessages(library('tidymodels'))
tar_option_set(
  controller = crew_controller_local(workers = 4),
  memory = "transient",
  garbage_collection = TRUE,
  storage = "worker",
  retrieval = "worker"
)

# options(future.globals.onReference = "error")
# plan(callr)

# Knobs to change/run things
duckpath = "[FILEPATH REDACTED]ducks/data.duckdb"
data_ver = 'd6' # increment this to indicate a refresh in noharms.identifiers
mod_ver = 'xenops' # version of the models
nmfq_version = 1 #increment this to refresh the first name frequency file
duckdir = '[FILEPATH REDACTED]ducks'
outfol = file.path('[FILEPATH REDACTED]', mod_ver)
dir.create(outfol, showWarnings = FALSE)
apply_screen = F
chksize = 1750000
zippath = "[FILEPATH REDACTED]Shapefiles_protected/ZIP/adci_wa_zip_confidential.shp"

tar_config_set(script = file.path(getwd(), '_targets.R'),
               store = file.path('[FILEPATH REDACTED]', '_targets/'))


# Sub model parameters
svm = (parsnip::svm_linear(mode = 'classification', engine = 'kernlab', cost = tune()))
rbf = parsnip::svm_rbf(mode = 'classification', engine = 'kernlab', cost = tune(), rbf_sigma = tune())
rf = (parsnip::rand_forest(mode = 'classification', trees = tune(), mtry = tune()))
xg = (set_engine(parsnip::boost_tree(mode = 'classification', tree_depth = tune(), mtry = tune(), trees = tune()), 
                 'xgboost', objective = 'binary:logistic'))
lg = (parsnip::boost_tree(mode = 'classification', tree_depth = tune(), mtry = tune(), trees = tune(), engine = 'lightgbm'))
nn = parsnip::bag_mlp('classification', hidden_units = tune(), penalty = tune(), epochs = tune())

s_params = tibble::tribble(~NAME, ~MODEL,
                           'svm', quote(svm),
                           'rf', quote(rf), 
                           'xg', quote(xg), 
                           'lg', quote(lg) ,
                           'rbf', quote(rbf)
                           # 'nn', quote(nn)
                           )

# Set target options:
tar_option_set(
  packages = c("data.table", 'DBI', 'glue', 'tools', 'duckdb',
               'kcgeocode', 'sf', 'stringr', 'hyrule', 'stringdist',
               'tidymodels', 'xgboost', 'kernlab', 'ranger',
               'workflows', 'stacks', 'dplyr', 'ggplot2',
               'igraph', 'keyring', 'bonsai', 'lightgbm', 'digest',
               'tokenizers', 'textreuse', 'baguette')
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()


# Other variables of interest
train_dsets = c(
  '[FILEPATH REDACTED]trainme/train_newhash.rds',
  '[FILEPATH REDACTED]trainme/pairs_7-2-2024.csv',
  '[FILEPATH REDACTED]trainme/pairs_7-3-2024.csv',
  '[FILEPATH REDACTED]trainme/pairs_7_5_2024.csv',
  '[FILEPATH REDACTED]trainme/pairs_07_16_2024.csv',
  '[FILEPATH REDACTED]trainme/mlvidh_rereview.csv',
  '[FILEPATH REDACTED]trainme/pairs_7-18-24_idhcomp-mongoose.csv',
  '[FILEPATH REDACTED]trainme/pairs_7-22-2024_2ndstageclustering.csv',
  '[FILEPATH REDACTED]trainme/pairs_7-22-2024_inducedlinks.csv',
  '[FILEPATH REDACTED]trainme/pairs_7-29-2024_bridges_turtle.csv',  
  '[FILEPATH REDACTED]trainme/pairs_7-30-24_mannetreview.csv'  
  
  
)

bounds = c(.01, .99)
form = pair ~ 
  dob_exact + dob_year_exact + dob_mdham + dob0101 +
  gender_agree + 
  first_name_jw + last_name_jw + name_swap_jw + complete_name_dl + middle_initial_agree + 
  last_in_last + first_is_middle + 
  ssn_full_exact + ssn_last4_exact + ssn_last4_dl +
  first_name_freq + last_name_freq +
  dob_freq + 
  # same_datasystem +
  mn_ziphist_distance + exact_location +
  ssn_any_lastname + ssn_any_firstname + ssid_any_firstname + ssid_any_lastname +
  nickname_match


  # missing_ssn + missing_zip + missing_ah
  # s_DEATH + s_HCHN + s_HMIS_CLIENT + s_KC_JAIL + s_MEDICAID + s_MEO + s_MUNI_JAIL +
  # s_PAO_DEF + s_PAO_VIC + same_datasystem + s_EMS #+ s_MEDICARE

# Set up duck db ----
setup = list(
  tarchetypes::tar_files_input(targetfilepath, "[FILEPATH REDACTED]no_harms/identity_matching/_targets.R"),
  tar_target(targetfile, list(file.copy(targetfilepath, file.path(outfol, '_targets_copy.R')), model_version)),
  tar_target(theform, form, deployment = 'main'),
  tar_target(data_version, data_ver, deployment = 'main'),
  tar_target(model_version, mod_ver, deployment = 'main'),
  # Initialize the duckdb with a mirror of noharms.identifiers
  tar_target(name = ducky, command = init_duck(duckpath, data_version), deployment = 'main'),
  tar_target(name = hashhist, command = upload_hash_hist(ducky, data_version), deployment = 'main'),
  
  # Create the first name frequency table
  tar_target(name = ssn_nm, command = create_ssn_name_lists(ducky, data_version = data_version, hashhist), deployment = 'main'),
  tar_target(name = nmfq, command = create_name_frequency(ducky, data_version = data_version, ssid_nm), deployment = 'main'),
  tar_target(name = ssid_nm, command = create_ssid_name_lists(ducky, data_version = data_version, ssn_nm), deployment = 'main'),
  
  # Add ZIP code geometries to table
  tar_target(name = zippy, add_zips_to_db(ducky, zippath, ssid_nm), deployment = 'main'),
  
  # Add nicknames
  tar_target(name = nicky, add_nicknames_to_db(ducky, data_version, zippy), deployment = 'main'),
  
  # Load/compile training data and save it to the duck
  #TODO: Take this out of stuff since it keeps invalidating or whatever
  tarchetypes::tar_files_input(tdsets, train_dsets),
  tar_target(name = trainonme, command = compile_training_data(tdsets, 
                                                               ducky, 
                                                               model_version,
                                                               theform,
                                                               test_frac = .15,
                                                               data_version = data_version,
                                                               zippy,
                                                               nmfq, 
                                                               nicky), deployment = 'main'),
  
  tar_target(uptrain, upload_training_data(ducky, trainonme, test = TRUE), deployment = 'main'),
  tar_target(train_blk_trigger, uptrain[[3]], deployment = 'main')
)


# Do blocking ----
block_setup = list(
  tarchetypes::tar_group_by(qgrid, command = make_query_grid(ducky, train_blk_trigger), qid), #lsh_results
  tar_target(qgrid_nrow, nrow(qgrid)),
  tar_target(bvars, command = make_block_var_table(ducky, data_version = data_version, qgrid_nrow)) #lsh_results
)

pairs4eval = list(
  tar_target(bpairs, make_block(ducky, qgrid, outfol, data_version = data_version, bvars, qgrid_nrow), pattern = map(qgrid), format = 'file', deployment = 'main')
)

# 
blocks = list(
  tar_target(blkz, command = block(ducky, bpairs, bvars, data_version = data_version)) ,
  tar_target(bids, create_bids(ducky,
                                      blktab = DBI::Id(table = paste0('blocks_', data_version)),
                                      chksize = chksize, blkz))
)

# Do modelling ----
fit_mods = list(
  ## Fit screener
  tar_target(screener, command = fit_screening_model(ducky, trainonme[[1]], 
                                                     bounds, theform, blkz), deployment = 'main'),
  ## Generate folds
  tar_target(tfolds, command = make_folds(ducky, trainonme[[1]], screener), deployment = 'main')
)
submods = tarchetypes::tar_map(
  values = s_params,
  names = 'NAME',
  tar_target(submod, fit_submodel(ducky, trainonme[[1]], screener, tfolds, MODEL, theform, apply_screen = apply_screen))
)
stackmod = tarchetypes::tar_combine(stk,
                                    submods,
                                    command = create_stacked_model(!!!.x,
                                                                   mnames = s_params$NAME,
                                                                   screener = screener))
# submods = list(tar_target(submod_lg, fit_submodel(ducky, trainonme[[1]], screener, tfolds, lg, theform, apply_screen = apply_screen, stacked = FALSE)))
# 
# stackmod = list(tar_target(stk, create_stacked_model(submod_lg, mnames = 'lg', screener = screener, singlemod = T)))

savestk = tar_target(sstk, command = saveobj(stk, file.path(outfol, 'stk.rds')), format = 'file', deployment = 'main')


all_preds = list(tar_target(preds,
                               predict_links(ducky,
                                             sstk, 
                                             DBI::Id(table = paste0('blocks_', data_version)),
                                             bids,
                                             outfol,
                                             data_version = data_version),
                               pattern = map(bids),
                               iteration = 'list',
                               format = 'file')
                    )


fixed_preds = list(tar_target(fpreds, fixed_links(ducky, sstk, ofol = outfol, data_version = data_version), format = 'file'))

cutoff_df = data.table::data.table(iter = 1:5, name = as.character(1:5))
cv_cutoffs = tarchetypes::tar_map(
  values = cutoff_df,
  names = 'name',
  tar_target(cv_co, cv_refit(ducky, sstk, model_version, iteration = iter, apply_screen = apply_screen, blkz))
)
combine_cutoffs = tarchetypes::tar_combine(cutme, 
                                           cv_cutoffs, 
                                           command = identify_cutoff(!!!.x, duck = ducky, testtab = trainonme[[3]], mods = sstk))

save_cut = tar_target(savecut, {
  saveRDS(cutme, file.path(outfol, paste0('cutoff_metrics_', model_version, '.rds')))
  file.path(outfol, paste0('cutoff_metrics_', model_version, '.rds'))
  }, format = 'file')

upres = tarchetypes::tar_combine(uped, all_preds, fixed_preds,
                                          command = upload_results(!!!.x,
                                                                   model_version = model_version))

erconnect = tar_target(ems_rhino, ems_rhino_fixed(ducky, sstk, uped[[2]], data_version = data_version,  uped[[1]]))
mdconnect = tar_target(meo_death, meo_death_fixed(ducky, sstk, uped[[2]], data_version = data_version, uped[[1]]))


fc1 = tar_target(comp1, reconcile_links(uped[[2]], cutpoint = cutme[['cutpoint']],
                                        model_version = model_version,
                                        ofol = outfol, triggers = list(ems_rhino, uped, meo_death)), format = 'file')

cc1 = list(tar_target(upcomp1, collapse_links(comp1,
                                             idtab = DBI::Id(table = paste0('data_', data_version)),
                                             idcol = c('main_id', 'id'),
                                             model_version = model_version,
                                             con = 'duck', duck = ducky)))
cdiags = list(tar_target(up_unclus_diag, upload_table(readRDS(comp1)[[2]], DBI::Id(schema = 'noharms', table = paste0(model_version, '_components_diag_unclustered')))),
              tar_target(up_clus_diag, upload_table(readRDS(comp1)[[3]], DBI::Id(schema = 'noharms', table = paste0(model_version, '_components_diag_clustered')))))

fc2 = tar_target(comp2, reconcile_links(uped[[2]], cutpoint = cutme[['cutpoint']],
                                        model_version = model_version,
                                        recursive = T,
                                        ofol = outfol, triggers = list(ems_rhino, uped, meo_death)), format = 'file')
cc2 = list(tar_target(upcomp2, collapse_links(comp2,
                                              idtab = DBI::Id(table = paste0('data_', data_version)),
                                              idcol = c('main_id', 'id'),
                                              model_version = model_version,
                                              con = 'duck', duck = ducky,
                                              tablename = paste0(model_version, '_components_cuttest'))))


identity_tab = tar_target(identity_table, make_identity_table(upcomp1[[2]], model_version, DBI::Id(column = 'aid_comp_id')))

fphs_tab = tar_target(fphs_table, make_fphs_table(identity_table))


# Process list ----
list(setup,
     # lsh,
     block_setup,
     pairs4eval,
     blocks,
     fit_mods,
     submods, 
     stackmod, 
     savestk,
     all_preds,
     # within_preds,
     # between_preds,
     fixed_preds,
     cv_cutoffs,
     combine_cutoffs,
     upres,
     erconnect,
     mdconnect,
     fc1,
     cc1,
     # fc2,
     # cc2,
     save_cut,
     cdiags,
     identity_tab,
     fphs_tab) 
