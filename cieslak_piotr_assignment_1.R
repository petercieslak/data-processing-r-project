### Data Processing in R and Python 2022Z
### Homework Assignment no. 1
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
### Report should include:
### * source() of this file at the beggining,
### * reading the data, 
### * attaching the libraries, 
### * execution time measurements (with microbenchmark),
### * and comparing the equivalence of the results,
### * interpretation of queries.

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#

sqldf_1 <- function(Posts){
  sqldf('SELECT STRFTIME("%Y", CreationDate) AS Year, COUNT(*) AS TotalNumber
   FROM Posts
   GROUP BY Year')
}

base_1 <- function(Posts){
  Posts_tmp <- Posts[, c('CreationDate', 'Id')]
  
  # format date to get year
  Posts_tmp[,1] = substr(Posts_tmp[,1], start=1, stop=4)
  
  # group by and change names
  setNames(aggregate(
    Posts_tmp$Id,
    Posts_tmp[,c('CreationDate'), drop=F],
    length
  ), c("Year", "TotalNumber"))
}

dplyr_1 <- function(Posts){
  Posts %>% dplyr::group_by(Year = substr(Posts[,4], start=1, stop=4)) %>%
    count(name = "TotalNumber")
}

data.table_1 <- function(Posts){
  Posts_dt <- as.data.table(Posts)
  Posts_dt[,.(TotalNumber=.N), by=(Year=substr(Posts[,4], start=1, stop=4))]
}

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_2 <- function(Users, Posts){
  sqldf(
  'SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
   FROM Users
   JOIN (
   SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
   ) AS Questions
   ON Users.Id = Questions.OwnerUserId
   GROUP BY Id
   ORDER BY TotalViews DESC
   LIMIT 10'
  )
}

base_2 <- function(Users, Posts){
 #select OwnerUserId and ViewCount from posts with id = 1 
 Questions <- na.omit(Posts[Posts$PostTypeId == 1, c('OwnerUserId', 'ViewCount')])

 #merge Users with Questions on Id/OwnerUserId 
 QuestionsUsersJoin <- merge(Users, Questions, by.x='Id', by.y='OwnerUserId')
 
 #groupy the merged table by Id
 result <- aggregate(
   QuestionsUsersJoin$ViewCount,
   QuestionsUsersJoin[,c('Id', 'DisplayName'), drop=F],
   function(x) {
      TotalViews = sum(x); 
   }
 )
 result <- result[order(-result$x),]
 rownames(result) <- NULL
 setNames(head(result,10),c('Id', 'DisplayName', 'TotalViews'))
}

dplyr_2 <- function(Users, Posts){
  #select OwnerUserId and ViewCount from posts with id = 1
  Posts %>% filter(PostTypeId == 1) %>% select(OwnerUserId, ViewCount) -> Questions
  
  #default sql join is inner join so we have to use inner_join dplyr function
  Users %>% inner_join(Questions, by=c("Id" = "OwnerUserId")) %>%
    select(Id, DisplayName, ViewCount) %>% group_by(Id) %>% 
    summarise(DisplayName = first(DisplayName), TotalViews = sum(ViewCount)) %>%
    arrange(desc(TotalViews)) %>% slice(1:10)
}

data.table_2 <- function(Users, Posts){
   Users_dt <- as.data.table(Users)
   Posts_dt <- as.data.table(Posts)
   Questions <- Posts_dt[PostTypeId == 1,.(OwnerUserId, ViewCount)]
   result <- na.omit(Users_dt[Questions
                      ,on = .(Id = OwnerUserId)][
                      ,.(Id, DisplayName, ViewCount)][
                      ,TotalViews := sum(ViewCount)
                      , by = Id][
                      , ViewCount := NULL][order(-TotalViews)])
   
   #Some rows are multiplied so I used unique() function to get one instance of a row.
   result <- unique(result, by = "Id")
   result[1:10,]
}

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

sqldf_3 <- function(Badges){
  sqldf(
  'SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
   FROM (
   SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
   FROM (
   SELECT Name, COUNT(*) AS Count, STRFTIME("%Y", Badges.Date) AS Year
   FROM Badges
   GROUP BY Name, Year
   ) AS BadgesNames
   JOIN (
   SELECT COUNT(*) AS CountTotal, STRFTIME("%Y", Badges.Date) AS Year
   FROM Badges
   GROUP BY YEAR
   ) AS BadgesYearly
   ON BadgesNames.Year = BadgesYearly.Year
   )
   GROUP BY Year'
  ) 
}

base_3 <- function(Badges){
  Badges_tmp <- Badges
  Badges_tmp[,4] <- substr(Badges[,4], start=1, stop=4)
  
  #Group badges by Date
  BadgesYearly <- aggregate(
    Badges_tmp$Id,
    by=Badges_tmp[,'Date', drop=F],
    length
  )
  colnames(BadgesYearly) <- c('Year', 'CountTotal')
  
  BadgesNames <- aggregate(
    Badges_tmp$Name,
    Badges_tmp[,c('Name', 'Date'), drop=F],
    length
  )
  colnames(BadgesNames) <- c('Name', 'Year', 'Count')
  
  YearlyNamesJoin <- merge(
    BadgesNames,
    BadgesYearly,
    by.x ='Year',
    by.y ='Year'
  )
  
  #This aggregate returns max percentage of each row grouped by Year column
  aggrYear <- aggregate(
    YearlyNamesJoin$Count / YearlyNamesJoin$CountTotal ~ YearlyNamesJoin$Year,
    YearlyNamesJoin,
    max
  )
  
  colnames(aggrYear) <- c('Year', 'MaxPercentage')
  
  #Another aggregate is needed in order to get Name column
  aggrYearName <- aggregate(
    YearlyNamesJoin$Count / YearlyNamesJoin$CountTotal ~ YearlyNamesJoin$Name + Year,
    YearlyNamesJoin,
    max
  )
  
  colnames(aggrYearName) <- c('Name', 'Year', 'MaxPercentage')
  
  #Merge the result of two aggregates to get the expected result
  result <- merge(aggrYearName, aggrYear, by.x='MaxPercentage', by.y='MaxPercentage')
  colnames(result) <- c('MaxPercentage', 'Name', 'Year')
  result <- result[,c(3,2,1)]
  result <- result[order(result$Year),]
  result
}

dplyr_3 <- function(Badges){
  
  BadgesNames <- rename(Badges, Year = Date) %>%
  select(Name, Year) %>%
  group_by(Year = substr(Badges[,4], start=1, stop=4), Name) %>% count() %>%
  rename(Count = n) %>% relocate(Year, .after = Count)
  
  BadgesYearly <- rename(Badges, Year = Date) %>%
  select(Year) %>%
  group_by(Year = substr(Badges[,4], start=1, stop=4)) %>% count() %>%
  rename(CountTotal = n) %>% relocate(Year, .after = CountTotal)
  
  result <- inner_join(BadgesNames, BadgesYearly, by='Year') %>%
  select(Year, Name, Count, CountTotal) %>%
  mutate(MaxPercentage = Count/CountTotal) %>%
  select(Year, Name, MaxPercentage) %>% group_by(Year) %>%
  slice(which.max(MaxPercentage))
  
  result
  }

data.table_3 <- function(Badges){
  Badges_dt <- as.data.table(Badges)
  BadgesYearly_dt <- Badges_dt[
  , `:=`(Year = substr(Badges[,4], start=1, stop=4))][
  , .(CountTotal=.N), by=Year]
  BadgesNames_dt <- Badges_dt[
  , `:=`(Year = substr(Badges[,4], start=1, stop=4))][
  , .(Name, Count=.N), by=list(Year, Name)][
  BadgesYearly_dt, on=.(Year=Year)][,Name.1:=NULL]
  result_dt <- BadgesNames_dt[, MaxPercentage:=Count/CountTotal][
  , .SD[which.max(MaxPercentage)], by=Year][
  , `:=`(Count = NULL, CountTotal = NULL)]
  result_dt
  }

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

sqldf_4 <- function(Comments, Posts, Users){
  sqldf(
  'SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
   FROM (
   SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
   CmtTotScr.CommentsTotalScore
   FROM (
   SELECT PostId, SUM(Score) AS CommentsTotalScore
   FROM Comments
   GROUP BY PostId
   ) AS CmtTotScr
   JOIN Posts ON Posts.Id = CmtTotScr.PostId
   WHERE Posts.PostTypeId=1
   ) AS PostsBestComments
   JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
   ORDER BY CommentsTotalScore DESC
   LIMIT 10'
  ) 
}

base_4 <- function(Comments, Posts, Users){
  
  CmtTotScr <- aggregate(
    Comments$Score ~ Comments$PostId,
    Comments,
    sum
  )
  
  colnames(CmtTotScr) <- c('PostId', 'CommentsTotalScore')
  
  Posts_tmp <- Posts[Posts$PostTypeId == 1,]
  PostsBestComments_merge <- merge(Posts_tmp, CmtTotScr,
                                   by.x='Id', by.y='PostId')
  PostsBestComments <- PostsBestComments_merge[,c(8,12,15,6,23)]
  
  
  Pbc_merge <- merge(PostsBestComments, Users, by.x='OwnerUserId', by.y='Id',
                     suffixes = c(".pbc", ".users"))
  
  result <- Pbc_merge[,c('Title', 'CommentCount', 'ViewCount',
                         'CommentsTotalScore', 'DisplayName', 'Reputation',
                         'Location')]
  
  result <- result[order(-result$CommentsTotalScore),]
  result[1:10,]
}

dplyr_4 <- function(Comments, Posts, Users){
  
  CmtTotScr <- Comments %>% group_by(PostId) %>%
    summarise(CommentsTotalScore = sum(Score)) %>%
    select(PostId, CommentsTotalScore)
   
  PostBestComments <- Posts %>% filter(PostTypeId == 1) %>%
    inner_join(CmtTotScr, by = c("Id" = "PostId")) %>%
    select(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore)
    
  result <- PostBestComments %>%
    inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
    select(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, 
           Reputation, Location) %>% arrange(desc(CommentsTotalScore)) %>%
    slice(1:10)
  
}

data.table_4 <- function(Comments, Posts, Users){
  Comments_dt <- as.data.table(Comments)
  Posts_dt <- as.data.table(Posts)
  Users_dt <- as.data.table(Users)
  
  CmtTotScr <- Comments_dt[, CommentsTotalScore := sum(Score), by=PostId][
    ,c("PostId", "CommentsTotalScore")]
  
  PostsBestComments <- Posts_dt[PostTypeId %in% 1,][
    CmtTotScr, on=.(Id = PostId)][
    ,c("OwnerUserId", "Title", "CommentCount",
       "ViewCount", "CommentsTotalScore")]
  
  result <- na.omit(PostsBestComments[Users_dt, on=.(OwnerUserId=Id)][
    ,c("Title", "CommentCount", "ViewCount", "CommentsTotalScore",
       "DisplayName", "Reputation", "Location")])
  result <- setorder(unique(result), -CommentsTotalScore)
  result[1:10,]
}

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_5 <- function(Posts, Votes){
  sqldf(
  'SELECT Posts.Title, STRFTIME("%Y-%m-%d", Posts.CreationDate) AS Date, VotesByAge.*
   FROM Posts
   JOIN (
   SELECT PostId,
   MAX(CASE WHEN VoteDate = "before" THEN Total ELSE 0 END) BeforeCOVIDVotes,
   MAX(CASE WHEN VoteDate = "during" THEN Total ELSE 0 END) DuringCOVIDVotes,
   MAX(CASE WHEN VoteDate = "after" THEN Total ELSE 0 END) AfterCOVIDVotes,
   SUM(Total) AS Votes
   FROM (
   SELECT PostId,
   CASE STRFTIME("%Y", CreationDate)
   WHEN "2022" THEN "after"
   WHEN "2021" THEN "during"
   WHEN "2020" THEN "during"
   WHEN "2019" THEN "during"
   ELSE "before"
   END VoteDate, COUNT(*) AS Total
   FROM Votes
   WHERE VoteTypeId IN (3, 4, 12)
   GROUP BY PostId, VoteDate
   ) AS VotesDates
   GROUP BY VotesDates.PostId
   ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
   WHERE Title NOT IN ("") AND DuringCOVIDVotes > 0
   ORDER BY DuringCOVIDVotes DESC, Votes DESC
   LIMIT 20'
  ) 
}

base_5 <- function(Posts, Votes){
  VoteDate <- character(nrow(Votes))
  
  #It is easier to set every row as before and then change values of the rows to during and after
  VoteDate <- rep('before', nrow(Votes))
  VotesYears <- substr(Votes[,4], start=1, stop=4)
  VoteDate[VotesYears == '2022'] <- 'after'
  VoteDate[VotesYears == '2021' | VotesYears == '2020'
           | VotesYears == '2019'] <- 'during'
  
  VotesCbind <- cbind(Votes, VoteDate)
  VotesCbind <- VotesCbind[VotesCbind$VoteTypeId %in% c(3, 4, 12),]
  VotesDates <- aggregate(
    VotesCbind$Id ~ VotesCbind$PostId + VotesCbind$VoteDate,
    VotesCbind,
    length
  )
  colnames(VotesDates) <- c("PostId", "VoteDate", "Total")
  
  VotesDatesBefore <- VotesDates[VotesDates$VoteDate == 'before',]
  VotesDatesDuring <- VotesDates[VotesDates$VoteDate == 'during',]
  VotesDatesAfter <- VotesDates[VotesDates$VoteDate == 'after',]
  
  VotesDatesBeforeAggr <- aggregate(
    VotesDatesBefore$Total ~ VotesDatesBefore$PostId,
    VotesDatesBefore,
    max
  )
  
  colnames(VotesDatesBeforeAggr) <- c("PostId", "BeforeCOVIDVotes")
  
  VotesDatesDuringAggr <- aggregate(
    VotesDatesDuring$Total ~ VotesDatesDuring$PostId,
    VotesDatesDuring,
    max
  )
  
  colnames(VotesDatesDuringAggr) <- c("PostId", "DuringCOVIDVotes")
  
  VotesDatesAfterAggr <- aggregate(
    VotesDatesAfter$Total ~ VotesDatesAfter$PostId,
    VotesDatesAfter,
    max
  )
  
  colnames(VotesDatesAfterAggr) <- c("PostId", "AfterCOVIDVotes")
  
  VotesByAge <- merge(VotesDatesBeforeAggr, VotesDatesDuringAggr, by="PostId",
                      all=TRUE)
  VotesByAge <- merge(VotesByAge, VotesDatesAfterAggr, by="PostId", all=TRUE)
  
  #Change NA, which have been created when merging unexisting rows, to 0
  VotesByAge[is.na(VotesByAge)] <- 0
  VotesByAge$Votes <- rowSums(VotesByAge[,c(2,3,4)])
  
  result <- merge(Posts, VotesByAge, by.x="Id", by.y="PostId")
  result <- result[,c("Title", "CreationDate", "Id", "BeforeCOVIDVotes", 
                      "DuringCOVIDVotes", "AfterCOVIDVotes", "Votes")]
  colnames(result) <- c("Title", "Date", "PostId", "BeforeCOVIDVotes", 
                        "DuringCOVIDVotes", "AfterCOVIDVotes", "Votes")
  result[,2] <- substr(result[,2], start=1, stop=10)
  result <- result[!(result$Title == "") & result$DuringCOVIDVotes > 0,]
  result <- result[order(-result$DuringCOVIDVotes, -result$Votes),]
  result[1:20,]
}

dplyr_5 <- function(Posts, Votes){
  VotesDates <- Votes %>%
    mutate(VoteDate = case_when(
      substr(Votes[,4], start=1, stop=4) == "2022" ~ "after",
      substr(Votes[,4], start=1, stop=4) == "2021" ~ "during",
      substr(Votes[,4], start=1, stop=4) == "2020" ~ "during",
      substr(Votes[,4], start=1, stop=4) == "2019" ~ "during",
      TRUE ~ "before"
    )) %>% filter(VoteTypeId %in% c(3, 4, 12)) %>%
    group_by(PostId, VoteDate) %>% count() %>% rename(Total = n)
  
  VotesByAge <- VotesDates %>%
    mutate(BeforeCOVIDVotes = 
             if_else(VoteDate == "before", max(Total), 0L)
    ) %>%
    mutate(DuringCOVIDVotes = 
             if_else(VoteDate == "during", max(Total), 0L)
    ) %>%
    mutate(AfterCOVIDVotes = 
             if_else(VoteDate == "after", max(Total), 0L)
    ) %>% group_by(PostId) %>%
    select(PostId, BeforeCOVIDVotes, DuringCOVIDVotes, AfterCOVIDVotes) %>%
    mutate(Votes = BeforeCOVIDVotes + DuringCOVIDVotes + AfterCOVIDVotes)
  
  result <- VotesByAge %>% inner_join(Posts, by=c("PostId" = "Id")) %>%
    filter(!(Title == "") & DuringCOVIDVotes > 0) %>%
    mutate(CreationDate = substr(CreationDate, start=1, stop=10)) %>%
    rename(Date = CreationDate) %>%
    select(Title, Date, PostId, BeforeCOVIDVotes, DuringCOVIDVotes, AfterCOVIDVotes,
           Votes) %>%
    arrange(desc(DuringCOVIDVotes), desc(Votes)) %>%
    head(20)
}

data.table_5 <- function(Posts, Votes){
  Posts_dt <- as.data.table(Posts)
  Votes_dt <- as.data.table(Votes)
  
  VotesDates <- Votes_dt[, Year := substr(CreationDate, start = 1, stop = 4)][
      , VoteDate := "before"][Year == "2022", VoteDate := "after"][
      Year == "2021" | Year == "2020" | Year == "2019", VoteDate := "during"][
      VoteTypeId == 3 | VoteTypeId == 4 | VoteTypeId == 12, .(PostId, VoteDate)][
      ,.(Total = .N), by=list(PostId, VoteDate)]
  
  VotesByAge <- VotesDates[VoteDate=="before",BeforeCOVIDVotes := max(Total), by=PostId][
    VoteDate=="during",DuringCOVIDVotes := max(Total), by=PostId][
    VoteDate=="after",AfterCOVIDVotes := max(Total), by=PostId]
  VotesByAge[is.na(VotesByAge)] <- 0
  VotesByAge <- VotesByAge[
    ,Votes := BeforeCOVIDVotes + DuringCOVIDVotes + AfterCOVIDVotes][
    ,.(PostId, BeforeCOVIDVotes, DuringCOVIDVotes, AfterCOVIDVotes, Votes)]
  
  result <- Posts_dt[VotesByAge, on=.(Id=PostId)][
    !(Title == "") & DuringCOVIDVotes > 0,
    .(Title, Date=substr(CreationDate, start=1, stop=10),
      Id, BeforeCOVIDVotes, DuringCOVIDVotes, AfterCOVIDVotes, Votes)]
  
  result <- result[, `:=`(BeforeCOVIDVotes=as.integer(BeforeCOVIDVotes),
                          DuringCOVIDVotes=as.integer(DuringCOVIDVotes),
                          AfterCOVIDVotes=as.integer(AfterCOVIDVotes))]
  
  setorder(result, -DuringCOVIDVotes, -Votes)
  setnames(result, c("Id"), c("PostId"))
  result[1:20,]
  
}

