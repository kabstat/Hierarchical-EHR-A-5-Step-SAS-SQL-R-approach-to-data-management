
library(readxl) # Used to load dataset
library(dplyr) # Used in Step 1 (Checks for duplicate rows) & Step 3 (Create derived variables from merged dataset)
library(sqldf) # Used in Step 2 (Merge both dataset) & 5 (Prepare hierarchical data for aggregation)
library(stringr) # Used in Step 3 (Create derived variables from merged dataset)
library(gmodels) # Used in Step 4 (QA derived variables from merged dataset (csection and delivery)


#### Import doctors data
doctors <- read_excel("/Users/KABSTAT/Downloads/ProgrammerTest/doctors.xlsx")
str(doctors)

#### Import mothers data
mothers <- read_excel("/Users/KABSTAT/Downloads/ProgrammerTest/mothers.xlsx")
str(mothers)

### Step 1
#### Check for duplicate rows (Mothers data set)
mothers_dup <- duplicated(mothers)
table(mothers_dup)

#### How many times is a row duplicated (numdup > 1) Mothers data set
dt <- aggregate(list(numdup=rep(1,nrow(mothers))), mothers, length)
# dt[order(dt$numdup), ]
tail(dt[order(dt$numdup), ])

#### Check for duplicate rows (Doctors data set)
doctors_dup <- duplicated(doctors)
table(doctors_dup)

#### How many times is a row duplicated (numdup > 1) Doctors data set
# How many times is a row duplicated
dt <- aggregate(list(numdup=rep(1,nrow(doctors))), doctors, length)
# dt[order(dt$numdup), ]
tail(dt[order(dt$numdup), ])

### Step 2
#### Merge both dataset (unique rows)
doctors_unique <- unique(doctors)
mothers_unique <- unique(mothers)

library(sqldf)

## Left join 
leftjoin <- sqldf("SELECT *
              FROM doctors_unique
              LEFT JOIN mothers_unique USING(Mother_ID)")
head(leftjoin)

### Step 3 
#### Create derived variables from merged dataset
leftjoin <- leftjoin %>% mutate(delivery_PR1 = ifelse(str_detect(PR1, c("720|721|724|726|728|729|731|733|736|738|740|741|742|744")), "yes", "no")) %>% mutate(delivery_PR2 = ifelse(str_detect(PR2,c("720|721|724|726|728|729|731|733|736|738|740|741|742|744")), "yes", "no")) %>% mutate(csection_PR1 = ifelse(str_detect(PR1, c("740|741|742|744")), "yes", "no")) %>% mutate(csection_PR2 = ifelse(str_detect(PR2, c("740|741|742|744")), "yes", "no"))
str(leftjoin)

#### Create derived variables from merged dataset (prepare variables for aggregate)
leftjoin2 <- leftjoin %>% mutate(delivery = ifelse((delivery_PR1=="yes" | delivery_PR2=="yes"), 'yes', 'no')) %>% mutate(csection = ifelse((csection_PR1=="yes" | csection_PR2=="yes"), 'yes', 'no')) %>% mutate(black = ifelse((RACE=="B"), 'black', 'not black')) 

### Step 4 
#### QA derived variables from merged dataset (csection and delivery)
CrossTable(leftjoin2$csection, leftjoin2$delivery, prop.t=FALSE, prop.r=FALSE,
           prop.c=FALSE, prop.chisq = FALSE)

### Step 5 
#### Prepare hierarchical data for aggregation
leftjoin3 <- leftjoin2 %>% mutate(delivery = ifelse((delivery_PR1=="yes" | delivery_PR2=="yes"), 1, 0)) %>% mutate(csection = ifelse((csection_PR1=="yes" | csection_PR2=="yes"), 1, 0)) %>% mutate(black = ifelse((RACE=="B"), 1, 0)) 
str(leftjoin3)

#### Use SQL for data aggregation
# write a full aggregation command, grouping by your specified columns
finaldata <- sqldf( "select Doctor_ID,
                    count(*) as aggregate,
		                sum(delivery) as sum_delivery, 
		                sum(csection) as sum_csection, 
		                sum(black) as sum_black 
             from leftjoin3 
             group by Doctor_ID" )

# print your result showing doctors with more deliveries
finaldata[order(finaldata$aggregate), ]

#### QA
# write a full aggregation command, grouping by your specified columns
QA_finaldata <- sqldf( "select count(*) as physician_count,
                    avg(sum_delivery) as mean_delivery_norm, 
			              avg(sum_csection) as mean_c_section, 
                    avg(sum_black) as mean_black
             from finaldata" )

# print your result
QA_finaldata
