# CLEANER - Imports and cleans impression-sale file, ownership file
# saves everything as lists and as .RData

# Require libraries # ------------------------------
require(reshape)
require(sqldf)
require(plyr)
require(foreach)
require(doParallel)

# Reference Tables from Data # ------------------------------
# rm(list=ls(all=TRUE))
load("~/Dropbox/Data/ref_tables.RData")

# Data Cleaning Functions # ------------------------------
archive_sampler <- function(filename, pretty_name, head) {
  
  setwd("~/Desktop/Data/")
  system(paste("cp original_files/",filename," clean_data/", sep=""))
  setwd("~/Desktop/Data/clean_data/")
  
  # unarchive full file
  if (grepl(".zip", filename)) {
    system(paste("unzip ",filename,sep=""))
    system(paste("rm ",filename,sep=""))
    filename = sub(".zip", ".txt", filename)
  } else if (grepl(".txt.gz", filename)) {
    system(paste("gunzip ",filename,sep=""))
    filename = sub(".txt.gz", ".txt", filename)
  } else { stop(simpleError("Unrecognized format! Use .zip or txt.gz")) }
  
  # sample head and save as pretty_name, delete full file
  if (head == "all") { system(paste("mv ",filename," ",pretty_name,sep=""))
  } else if(!is.na(as.numeric(head))) {
    options(scipen=10)
    system(paste("head -",as.character(round(as.numeric(head)))," ",filename," > ",pretty_name,sep=""))
    system(paste("rm ",filename,sep=""))
  } else { stop(simpleError("head must be numeric or 'all'!"))}
  
} # unarchives and copies -head lines from file
prepare_salesmatch <- function(filename) {
  
  # cut views and sales to be stored separately
  system(paste("cut -d '^' -f 1,4,40-45 ",filename," > views",sep=""))
  system(paste("cut -d '^' -f 2-13,15-26,28-40 ",filename," > sales",sep=""))
  system(paste("rm ",filename,sep=""))
  
  # grab headers
  system("head -1 views > orig_views_header")
  system("tail -n +2 views > views_nh")
  system("head -1 sales > orig_sales_header")
  system("tail -n +2 sales > sales_nh")
  system("rm views sales")
  
  # sort views by make, model, anon_code, site_section
  system("sort -t^ -k 4,4 -k 5,5 -k 3,3 -k 6,6 views_nh > views")
  system("rm views_nh")
  
  # select non-empty records and dedoop
  system("sed '/^\\^/ d' sales_nh > temp")
  system("uniq temp > sales")
  system("rm sales_nh temp")
  
  # normalize and sort sales
  system("cut -d '^' -f 37,1-12 orig_sales_header > norm_sales_header")
  system("cut -d '^' -f 37,1-12 sales > V1")
  system("cut -d '^' -f 37,13-24 sales > V2")
  system("cut -d '^' -f 37,25-36 sales > V3")
  system("cat V1 V2 V3 > temp")
  system("rm V1 V2 V3")
  system("sed '/^\\^/ d' temp > sales")
  system("sort -t^ -k 13,13 -k 1,1 -k 2,2 -k 3,3 sales > temp")
  system("uniq temp > sales")
  system("rm temp")
  
  # replace headers on top
  system("cat norm_sales_header sales > dirty_sales.txt")
  system("cat orig_views_header views > dirty_views.txt")
  system("rm views sales orig_sales_header norm_sales_header")
  
} # normalizes salesmatch using the cmdln
prepare_own <- function(filename) {
  
  # remove views and dedoop ownership
  system(paste("cut -d '^' -f 7-56,1 ",filename," > temp",sep=""))
  system(paste("rm ",filename,sep=""))
  system("uniq temp > own")
  system("rm temp")
  
  # grab header
  system("head -1 own > orig_own_header")
  system("tail -n +2 own > own_nh")
  system("rm own")
  
  # normalize and dedoop
  system("awk 'length > 72 {print;}' own_nh > temp") # gets rid of empty records
  system("uniq temp > own_nh")
  system("rm temp")
  system("cut -d '^' -f 1-6 orig_own_header > norm_own_header")
  system("cut -d '^' -f 1-6 own_nh > V1")
  system("cut -d '^' -f 1,7-11 own_nh > V2")
  system("cut -d '^' -f 1,12-16 own_nh > V3")
  system("cut -d '^' -f 1,17-21 own_nh > V4")
  system("cut -d '^' -f 1,22-26 own_nh > V5")
  system("cut -d '^' -f 1,27-31 own_nh > V6")
  system("cut -d '^' -f 1,32-36 own_nh > V7")
  system("cut -d '^' -f 1,37-41 own_nh > V8")
  system("cut -d '^' -f 1,42-46 own_nh > V9")
  system("cut -d '^' -f 1,47-51 own_nh > V10")
  system("cat V1 V2 V3 V4 V5 V6 V7 V8 V9 V10 > own_nh")
  system("rm V1 V2 V3 V4 V5 V6 V7 V8 V9 V10")
  system("awk 'length > 25 {print;}' own_nh > temp")
  system("sort -t^ -k 1,1 -k 2,2 -k 3,3 temp > sort")
  system("rm temp")
  system("uniq sort > own_nh")
  system("rm sort")
  
  # replace header
  system("cat norm_own_header own_nh > dirty_own.txt")
  system("rm own_nh norm_own_header orig_own_header")
  
} # normalizes owner using the cmdln
prepare_demo <- function(filename) {
  # remove views
  # dedoop
  system(paste("mv",filename,"dirty_demo.txt",sep=" "))
} # dedoops demo (dummy)
prepare_loyal <- function(filename) {
  # remove views
  # dedoop
  system(paste("mv",filename,"dirty_loyal.txt",sep=" "))
} # dedoops loyal (dummy)
cmd_prepare <- function(f) {
  
  switch(f,
         salesmatch = prepare_salesmatch(f),
         own = prepare_own(f),
         demo = prepare_demo(f),
         loyal = prepare_loyal(f))
  return()
  
} # prepares any one of the four files with cmdln
power_wash <- function(dirty_record) {  
  dirty_make = tolower(dirty_record["make"])
  dirty_model = tolower(dirty_record["model"])
  
  # lookup clean record in the clean table
  matching_makes = clean_table[clean_table$dirty_make==dirty_make,]
  matching_models = matching_makes[matching_makes$dirty_model==dirty_model,]
  clean_record = clean_table[intersect(row.names(matching_makes), row.names(matching_models)),]
  
  #if its garbage, return OTHERs
  if (dim(matching_makes)[1] == 0) {
    clean_record[1,]= c(dirty_make, dirty_model, "OTHER", "OTHER", "OTHER")
    return (clean_record)
  }
  
  # if it found some clean make and now looks it up in the all table
  if (dim(clean_record)[1] == 0) { # if there is no make AND model match
    clean_make = matching_makes$make[1]
    all_lookup = make_all_table[make_all_table$make==clean_make,]
    if (dim(all_lookup)[1] == 0) { # safeguard if somehow it finds a clean_make but it's not in the all_lookup
      clean_record[1,] = c(dirty_make, dirty_model, "ALL", clean_make, "OTHER")
      return (clean_record)
    }
    all_id = all_lookup$id
    clean_record[1,] = c(dirty_make, dirty_model, "ALL", clean_make, all_id)
  }
  
  return (clean_record)
} # cleans make/model, tag logic issues
views_cleaner <- function (dirty_views) {
  # cast views
  views = data.frame(viewed_anon_code = as.character(dirty_views$ANON_CODE),
                     viewed_datediff = as.integer(dirty_views$DATEDIFF1),
                     purchase_date = as.Date(as.character(dirty_views$V1PURCHASE_DATE), "%m/%d/%Y"),
                     viewed_make = as.character(dirty_views$MAKE_VIEWED),
                     viewed_model = as.character(dirty_views$MODEL_VIEWED),
                     site_section = as.character(dirty_views$SITE_SECTION),
                     viewed_zip = as.character(dirty_views$ZIP),
                     viewed_intent = as.character(dirty_views$INTENT),
                     stringsAsFactors=FALSE)
  
  # power wash make/model
  make_models = sqldf("select distinct viewed_make as make, viewed_model as model from views")
  cleaned_records = do.call("rbind", apply(make_models, 1, power_wash))
  views = sqldf("select * from views as V
                left join cleaned_records as C
                on V.viewed_make=C.dirty_make and V.viewed_model=C.dirty_model")
  views[,c("viewed_make","viewed_model","viewed_id")] = views[,c("make","model","id")]
  
  # join segments
  views$viewed_make_model = paste(views$viewed_make, views$viewed_model, sep = " ")
  views = sqldf("select * from views as V
                left join segment_map as SM
                on V.viewed_make_model = SM.cars_make_model")
  views[,"viewed_JDPA_segment"] = views[,"JDPA_segment"]
  
  #calculate viewed dates
  views$viewed_date = views$purchase_date + views$viewed_date
  
  # join DMAs
  views = sqldf("select * from views as V
                left join zip_dma_map as ZM
                on V.viewed_zip = ZM.zip")
  views[,c("viewed_dma_code","viewed_county","viewed_dma_name")] = views[,c("dma_code","county","dma_name")]
  
  # join site_section buckets
  views = sqldf("select * from views
                left join site_section_buckets
                on views.site_section = site_section_buckets.section")
  
  # drop the old columns
  drops <- c("dirty_make", "dirty_model", "make", "model", "id", "zip", "dma_code", "county", "dma_name",
             "site_section", "viewed_make_model", "cars_make_model", "JDPA_segment")
  views = views[,!(names(views) %in% drops)]
  
  return(views)
} # casts views, cleans dates, make/model
splitter <- function(file_name_to_split, split_rows) {  
  # Split clean file into chunks
  cat("Splitting file...\n")
  system("mkdir Splits")
  options(scipen=10) #so that it prints the long way, how cmdln wants it
  system(paste("split -l",as.character(split_rows),as.character(file_name_to_split),"Splits/"))
  
  # Note filenames
  system("ls Splits > split_files.txt")
  split_file = as.character(read.csv("split_files.txt", header = FALSE)[[1]])
  cat("There are",as.character(length(split_file)),"files\n\n")
  
  return(split_file)
} # shape data chunks and destination
split_wrapper <- function (file_to_split, split_rows) {
  
  # split the file into /Splits
  split_file = splitter(file_to_split, split_rows)
  system(paste("rm ",file_to_split,sep=""))
  
  # Header for each chunk
  orig_col_names = names(read.csv("orig_views_header", header = TRUE, sep = "^"))
  system("rm orig_views_header")
  file_num = length(split_file)
  
  # Loop through the split files and go through entire process
  run_times = data.frame(time = numeric())
  for (i in 1:file_num) {
    ptm <- proc.time()
    
    cat("File",as.character(i),"of",as.character(file_num),"\n")
    file = paste("Splits/", split_file[i], sep="")
    if (i == 1) { dirty_views = read.delim(file, sep = "^", header = TRUE, colClasses = "character") }
    else { dirty_views = read.delim(file, sep = "^", header = FALSE, colClasses = "character") }
    colnames(dirty_views) = orig_col_names
    
    cat("Cleaning...\n")
    views = views_cleaner(dirty_views)
    
    # save chunk of views
    f = paste("views_", i, ".RData", sep="")
    save(views, file = f)
    
    # keep track and estimate time in minutes
    this_run = ((proc.time() - ptm)[3])/60
    run_times = rbind(run_times, this_run)
    total_time = sum(run_times)
    if (i < file_num) {
      estimated_time_remaining = (file_num-i) * mean(run_times[[1]])
      cat(total_time,"minutes have elpased\n")
      cat("There are an estimated",estimated_time_remaining,"minutes remaining\n\n")
    }
  }
  
  system("rm -R Splits")
  system("rm split_files.txt")
  
  return(file_num)
  
} # splits and calls views_cleaner for each chunk
sales_cleaner <- function (dirty_sales) {
  
  # cast sales into a friendly data frame
  sales = data.frame(purchase_anon_code = as.character(dirty_sales[["ANON_CODE"]]),
                     purchase_make = tolower(as.character(dirty_sales[["V1MAKE"]])),
                     purchase_model = tolower(as.character(dirty_sales[["V1MODEL"]])),
                     purchase_date = as.Date(as.character(dirty_sales[["V1PURCHASE_DATE"]]), "%m/%d/%Y"),
                     purchase_lease = as.character(dirty_sales[["V1PURCHASE_LEASE"]]),
                     purchase_newused = as.character(dirty_sales[["V1NEWUSED_INDICATOR"]]),
                     purchase_fuel = as.character(dirty_sales[["V1FUEL_CODE"]]),
                     purchase_modelyear = as.integer(dirty_sales[["V1MODEL_YEAR"]]),
                     dealer_name = as.character(dirty_sales[["V1DEALER_NAME"]]),
                     dealer_address = as.character(dirty_sales[["V1DEALER_ADDRESS"]]),
                     dealer_city = as.character(dirty_sales[["V1DEALER_CITY"]]),
                     dealer_state = as.character(dirty_sales[["V1DEALER_STATE"]]),
                     dealer_zip = as.character(dirty_sales[["V1DEALER_ZIP"]]))
  
  # power wash make/model
  make_models = sqldf("select distinct purchase_make as make, purchase_model as model from sales")
  cleaned_records = do.call("rbind", apply(make_models, 1, power_wash))
  sales = sqldf("select * from sales as S
                left join cleaned_records as C
                on S.purchase_make=C.dirty_make and S.purchase_model=C.dirty_model")
  sales[,c("purchase_make","purchase_model","purchase_id")] = sales[,c("make","model","id")]
  drops <- c("make","model","id", "dirty_make", "dirty_model")
  sales = sales[,!(names(sales) %in% drops)]
  
  # clean dealers (bastards)
  sales = sqldf("select * from sales as S left join dealer_table as C
                on S.dealer_name = C.dirty_name and
                S.dealer_address = C.dirty_address and
                S.dealer_city = C.dirty_city")
  sales[,c("dealer_id", "dealer_franch_ind", "dealer_status")] = sales[,c("id", "franch_ind", "status")]
  drops <- c("id", "name", "address", "zip", "city", "state", "county", "franch_ind", "status",
             "dirty_name", "dirty_address", "dirty_city")
  sales = sales[,!(names(sales) %in% drops)]
  
  # add DMAs for dealers
  sales = sqldf("select * from sales
                left join zip_dma_map
                on sales.dealer_zip = zip_dma_map.zip")
  sales[,c("dealer_dma_code","dealer_dma_name")] = sales[,c("dma_code","dma_name")]
  drops <- c("zip","dma_code","county","dma_name")
  sales = sales[,!(names(sales) %in% drops)]
  
  # join JDPA segments
  sales$purchase_make_model = paste(sales$purchase_make, sales$purchase_model, sep = " ")
  sales = sqldf("select * from sales
                left join segment_map
                on sales.purchase_make_model = segment_map.cars_make_model")
  sales[,"purchase_JDPA_segment"] = sales[,"JDPA_segment"]
  drops <- c("purchase_make_model", "cars_make_model", "JDPA_segment")
  sales = sales[,!(names(sales) %in% drops)]
  
  save(sales, file = "sales.RData")
  return(sales)
  
} # casts sales, identifies dealer_ids, make/model_ids
own_cleaner <- function (dirt_own) {
  
  # cast own into a friendly data frame
  own = data.frame(own_anon_code = as.character(dirty_own[["ANON_CODE"]]),
                   own_make = tolower(as.character(dirty_own[["V1MAKE"]])),
                   own_model = tolower(as.character(dirty_own[["V1MODEL"]])),
                   own_newused = as.character(dirty_own[["V1NEWUSED_INDICATOR"]]),
                   own_modelyear = as.integer(dirty_own[["V1MODEL_YEAR"]]),
                   own_disposed_date = as.Date(as.character(dirty_own[["V1DISPOSED_DATE"]]), "%m/%d/%Y"))
  
  # power wash make/models
  make_models = sqldf("select distinct own_make as make, own_model as model from own")
  cleaned_records = do.call("rbind", apply(make_models, 1, power_wash))
  own = sqldf("select * from own as S
              left join cleaned_records as C
              on S.own_make=C.dirty_make and S.own_model=C.dirty_model")
  own[,c("own_make","own_model","own_id")] = own[,c("make","model","id")]
  drops <- c("make","model","id", "dirty_make", "dirty_model")
  own = own[,!(names(own) %in% drops)]
  
  # join JDPA segments
  own$own_make_model = paste(own$own_make, own$own_model, sep = " ")
  own = sqldf("select * from own
              left join segment_map
              on own.own_make_model = segment_map.cars_make_model")
  own[,"own_JDPA_segment"] = own[,"JDPA_segment"]
  drops <- c("own_make_model", "cars_make_model", "JDPA_segment")
  own = own[,!(names(own) %in% drops)]
  
  save(own, file = "own.RData")
  return (own)
} # casts and cleans dirty_own
demo_cleaner <- function (dirty_demo) {
  demo = dirty_demo
  
  save(demo, file = "demo.RData")
  return(demo)
} # casts and cleans dirty_demo (dummy)
loyal_cleaner <- function (dirty_loyal) {
  loyal = dirty_loyal
  
  save(loyal, file = "loyal.RData")
  return(loyal)
} # casts and cleans dirty_loyal (dummy)

# Main Routine # ------------------------------
orig_filename = c("","","","","") # the structure has changed

# define the parameters for extraction
on = c(T, T, T, T)
pretty_name = c("salesmatch", "own", "demo", "loyal")
sample_size = c(1*10^7, "all", "all", "all")
file = cbind(orig_filename, pretty_name, sample_size)[on,]

# unarchive and prepare each file (only when more than one file is on)
apply(file, 1, function(f) {
  cat(paste("Preparing ",f["pretty_name"],"...\n",sep=""))
  archive_sampler(f["orig_filename"], f["pretty_name"], f["sample_size"])
  cmd_prepare(f["pretty_name"])
  cat(paste("Finished preparing ",f["pretty_name"],"!\n\n",sep=""))
  return()
})

# establish new names
on = c(rep(on[1],2), on[2:4])
file = c("views", "sales", "own", "demo", "loyal")
dirty_file = sapply(file, function (f) paste("dirty_",f,".txt",sep=""))

# clean each non-view file
if (on[2]) {
  dirty_sales = read.delim(dirty_file["sales"], header = T, sep = "^", colClasses = "character")
  sales = sales_cleaner(dirty_sales)
  system(paste("rm ",dirty_file["sales"],sep=""))
}
if (on[3]) {
  dirty_own = read.delim(dirty_file["own"], header = T, sep = "^", colClasses = "character")
  own = own_cleaner(dirty_own)
  system(paste("rm ",dirty_file["own"],sep=""))
}
if (on[4]) {
  dirty_demo = read.delim(dirty_file["demo"], header = T, sep = "^", colClasses = "character")
  demo = demo_cleaner(dirty_demo)
  system(paste("rm ",dirty_file["demo"],sep=""))
}
if (on[5]) {
  dirty_loyal = read.delim(dirty_file["loyal"], header = T, sep = "^", colClasses = "character")
  loyal = loyal_cleaner(dirty_loyal)
  system(paste("rm ",dirty_file["loyal"],sep=""))
}

# clean the large view file
if (on[1]) {
  cat("\n~View Cleaning by Chunks...\n")
  file_to_split = dirty_file["views"]
  split_rows = 8*10^5
  file_num = split_wrapper(file_to_split, split_rows)
  
  cat("Combining views...\n")
  load("views_1.RData")
  system("rm views_1.RData")
  all_views = views
  for (i in 2:file_num) {
    load(paste("views_",i,".RData",sep=""))
    system(paste("rm ","views_",i,".RData",sep=""))
    all_views = rbind(all_views, views)
  }
  rm(views)
  cat("Saving all views...\n")
  save(all_views, file = "views.RData")
}

cat("\n~DATA CLEANING COMPLETE~\n")
