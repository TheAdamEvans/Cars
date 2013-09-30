setwd("/Users/Adam/Dropbox/Cars.com/Results")
# takes analysis, outcomes from Logit_Regression_Analysis

# Additions
  # Automatic export
  # Add lower bound estimates to top ones
  # Skinny black bars
  # only show significant predictors

load("~/Dropbox/Cars.com/Data/pretty_name.RData")
load("~/Dropbox/Cars.com/Data/ordered_names.RData")

grab_importance <- function(summary) {
  
  importance = do.call("cbind", lapply(summary, function(var_data) {
    sorted = var_data[ order(row.names(var_data)), ]
    importance_bounds = sorted[c("upper","lower")]
    return(importance_bounds)
  }))
  
  return(importance)
  
}
cat_flipper <- function(all_importance) {
  
  cat.upper = list()
  for (var in outcomes) {
    cat.upper[[var]] = lapply(all_importance, function(importance_df) {
      return(importance_df[[paste(var,".upper",sep="")]])
    })
  }
  
  cat.lower = list()
  for (var in outcomes) {
    cat.lower[[var]] = lapply(all_importance, function(importance_df) {
      return(importance_df[[paste(var,".lower",sep="")]])
    })
  }
  
  stacker <- function(d) {
    t = do.call("cbind", d)
    row.names(t) = var_names
    return(as.data.frame(t))
  }
  
  stacked = list()
  stacked[["upper"]] = lapply(cat.upper, stacker)
  stacked[["lower"]] = lapply(cat.lower, stacker)
  
  return (stacked)
}
importance_plot <- function(left_hand, population) {
  
  # extract upper and lower bounds for one outcome in one population
  upper = cat[["upper"]][[left_hand]][[population]]
  lower = cat[["lower"]][[left_hand]][[population]]
  
  # add on pretty names and sort
  importance_se = cbind(upper, lower)
  row.names(importance_se) = pretty_name[["label"]]
  sort = importance_se[ordered_names,]
  
  # trick the plot dimensions into working
  chart_title = paste(left_hand, "in", population, "shoppers", sep = " ")
  sub_title = "Median P[left_hand] = 0.15"
  extent = apply(sort, 1, function(var) {
    min = min(var["upper"], var["lower"])
    max = max(var["upper"], var["lower"])
    return(cbind(min, max))
  })
  range = c(rep(min(extent[1,]), dim(extent)[2]),rep(max(extent[2,]), dim(extent)[2]))
  range_mat = matrix(range,
                 nrow = 2,
                 byrow = TRUE)
  range_mat[2,] = range_mat[2,] - range_mat[1,]
  
  # call the bar plots
  par(mar=c(6,7,4,1))
  barplot(range_mat,
          border="transparent",
          col = rgb(0,0,0,0),
          names.arg = row.names(sort),
          las = 1,
          horiz = TRUE,
          # sub = sub_title,
          main = chart_title)
  barplot(sort[,"upper"],
          #col = bar_colors(sort[,"upper"]),
          col = rgb(0,0,0, 0.5),
          las = 1,
          horiz = TRUE,
          add = TRUE)
  barplot(sort[,"lower"],
          #col = bar_colors(sort[,"lower"]),
          col = rgb(0,0,0, 0.5),
          las = 1,
          horiz = TRUE,
          add = TRUE)
  
}

# take the importance column from every var_data
importance = lapply(analysis, grab_importance)
var_names = row.names(importance$all)

# turn from a list across populations to a list across outcomes
cat = cat_flipper(importance)

# establish pretty names
pretty_name <- read.csv("~/Dropbox/Cars.com/Data/pretty_name.csv", colClasses = "character")

# prepare for feed to importance plot
left_hand = "purchase_ind"
population = "all"
importance_plot(left_hand, population)

interesting_populations = c("records_8", "premium", "loyalist_make", "loyalist_size")
# Crudely exports 2x2 plots of variable importance for each outcome
lapply(outcomes, function (left_hand) {
  par(mfrow=c(2,2))
  lapply(interesting_populations, function(pop) {
    importance_plot(left_hand, pop)
  })
  dev.copy2pdf(file = paste(left_hand,"pdf", sep="."))
})
