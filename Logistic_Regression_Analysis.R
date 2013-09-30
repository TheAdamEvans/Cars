# returns great summary tables of variables for each population for each fit
require(car)
require(ggplot2)
require(sqldf)

# Turn all the for loops into lapply!!
# Additions
  # unconditional expectation (mean probability)
  # median probability
  # size of sample, percent of sample (these 10,000 shoppers with >10 records represent only 8% of visitors)

logit_fitter <- function(shopper, outcomes) {
  
  # run the logistic regressions with different left hand sides
  fits = list()
  for (y in outcomes) {
    formula = paste(sep = " ~ ", y,
                    paste(sep = " + ", collapse = " + ",
                          section_var,
                          prop_var))
    fits[[y]] = glm(formula, data = shopper, family = "binomial")
  }
  
  return(fits)
  
} # fits a logit model for each outcome
logit_summary <- function(shopper_logical) {
  shopper_sample = shopper[shopper_logical,]
  
  # fit each outcome on table of features
  logit_fits = logit_fitter(shopper_sample, outcomes)
  
  # determine reasonable_change manually or otherwise
  reasonable_change = data.frame(reasonable_change = apply(shopper_sample[,variables], 2, sd))
  reasonable_change["lead", "reasonable_change"] = 1
  reasonable_change["contact", "reasonable_change"] = 1
  reasonable_change["config", "reasonable_change"] = 1
  reasonable_change["advice", "reasonable_change"] = 1
  reasonable_change["finance", "reasonable_change"] = 1
  
  # identify the medians and reasonable changes in each variable
  # mean = data.frame(mean = colMeans(shopper[,variables]))
  # sd = data.frame(sd = apply(shopper[,variables], 2, sd))
  median = data.frame(median = apply(shopper_sample[,variables], 2, median))
  
  # summary = lapply(outcomes, )
  summary = list()
  for (fit in outcomes) {
    # store coeffient matricies as a data.frame
    coef_matrix = summary(logit_fits[[fit]])$coefficients
    var_data = data.frame(coef_estimate = coef_matrix[-1,"Estimate"], # -1 exclude the intercept
                          std_error = coef_matrix[-1,"Std. Error"],
                          p_value = coef_matrix[-1,"Pr(>|z|)"])
    var_data$odds = exp(var_data$coef_estimate)
    # var_data$var_name = as.character(row.names(var_data))
    
    # calulate and join vif
    vif = data.frame(vif = vif(logit_fits[[fit]]))
    var_data = merge(var_data, vif, by = 0)
    row.names(var_data) = var_data$Row.names
    var_data = var_data[-1]
    
    # join medians
    var_data = merge(var_data, median, by = 0)
    row.names(var_data) = var_data$Row.names
    var_data = var_data[-1]
    
    # bring in reasonable_change
    var_data = merge(var_data, reasonable_change, by = 0)
    row.names(var_data) = var_data$Row.names
    var_data = var_data[-1]
    
    # develop notion of variable importance
    # importance = apply(var_data,1,change_predict)
    importance = data.frame(upper = numeric(dim(var_data)[1]),
                            lower = numeric(dim(var_data)[1]))
    row.names(importance) = row.names(var_data)
    for (i in 1:dim(var_data)[1]) {
      # establish which variable we're talking about
      var_row = var_data[i,]
      var_name = row.names(var_row)
      median_case = data.frame(t(median), row.names = var_name)
      orig_prob = predict(logit_fits[[fit]], newdata = median_case, type = "response")
      
      # make the reasonable change to the row_var
      new_case = median_case
      new_case[[var_name]] = new_case[[var_name]] + var_row[["reasonable_change"]]
      new_prob = predict(logit_fits[[fit]], newdata = new_case, type = "response", se.fit = TRUE)
      importance[var_name,"upper"] = ((new_prob$fit - orig_prob) + new_prob$se.fit) * 100
      importance[var_name,"lower"] = ((new_prob$fit - orig_prob) - new_prob$se.fit) * 100
    }
    
    # bring in importance
    var_data = merge(var_data, importance, by = 0)
    row.names(var_data) = var_data$Row.names
    var_data = var_data[-1]
    
    summary[[fit]] = var_data
  }
  
  return (summary)
} # logit regressions on shopper_sample for each outcome
shopper_filtering <- function(shopper) {
  shopper_list = list()
  shopper_list[["all"]] = rep(TRUE, dim(shopper)[1])
  
  # records conditions
  shopper_list[["records_4"]] = shopper["records"] > 4
  shopper_list[["records_8"]] = shopper["records"] > 8
  shopper_list[["records_16"]] = shopper["records"] > 16
  
  # prop conditions
  shopper_list[["loyalist_make"]] = shopper["prop_primary_make"] > 0.5
  shopper_list[["loyalist_model"]] = shopper["prop_primary_model"] > 0.5
  shopper_list[["loyalist_class"]] = shopper["prop_primary_segment_class"] > 0.5
  shopper_list[["loyalist_size"]] = shopper["prop_primary_segment_size"] > 0.5

  #   newused
  #   shopper_list[["intent_used"]] = shopper["viewed_used"] > 0.75
  #   shopper_list[["intent_new"]] = shopper["viewed_new"] > 0.75
  #   shopper_list[["intent_cpo"]] = shopper["viewed_cpo"] > 0.75
  
  # premium
     shopper_list[["premium"]] = shopper["premium"] > 0.75
  #  shopper_list[["super_premium"]] = shopper["premium"] > 0.9
  
#   no lead and/or no contact
  #shopper_list[["no_lead_no_contact"]] = (shopper["lead"] == 0 & shopper["contact"] == 0)
#   shopper_list[["no_lead"]] = shopper["lead"] == 0
#   shopper_list[["no_contact"]] = shopper["contact"] == 0
  
  return(shopper_list)
} # returns a variety indexes to different shopper groups
sample_sizer <- function(f) {
  size = dim(shopper[f,])[1] / population_size
  return(size)
} # returns the sample proportion to the whole population

# load("~/Desktop/Cars_Data/salesmatch/286/shopper_286.RData")

# define threshold for a real shopper
threshold = 2
shopper = all_shopper[all_shopper["records"] > threshold,]
population_size = dim(shopper)[1]

# should be in sale_analysis
shopper = cbind(shopper, purchase_ind = apply(shopper, 1, function(d) if (!is.na(d["purchase_make"])) return(1) else return(0)))

# create interesting populations
shopper_filter = shopper_filtering(shopper)

# define feature groups (right-hand side)
section_var = c("records","lead","contact","finance","advice","config","buy","research","premium")
prop_var = c("prop_primary_newused","prop_primary_segment_class","prop_primary_segment_size",
             "prop_primary_make", "prop_primary_model")
# owner var
# demo var
variables = c(prop_var, section_var) #owner_var, demo_var

# define outcomes (left-hand side)
outcomes = c("purchase_ind", "make_match", "model_match", "segment_match", "newused_match")

# calculate sample size
sample_size = do.call("c", lapply(shopper_filter, sample_sizer))

# summarize all outcomes over each sample
analysis = lapply(shopper_filter, logit_summary)