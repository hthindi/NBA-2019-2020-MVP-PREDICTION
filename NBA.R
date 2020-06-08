rm(list = ls())
library(dplyr)
library(odbc)
library(DBI)
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "DESKTOP-K12S3HQ",
                 Database = "NBA MVP"
)

years <- seq(2012,2019,1)
id = seq(1,length(years)-1)

data = data.frame()
for (i in id){
  data <- rbind(data,dbReadTable(con,paste0("vv_",years[i],"_",years[i+1],"_additional_stats")))
}


num_mvp_candidates <- c()
for (i in id){
  num_mvp_candidates <- append(num_mvp_candidates,nrow(dbReadTable(con,paste0("vv_",years[i],"_",years[i+1],"_additional_stats"))))
}

data$begin <- NA
data$end <- NA

begin <- c()
for (i in id){
  begin <- append(begin,rep(years[i],num_mvp_candidates[i]))
}
data$begin <- begin

end <- c()
for (i in id){
  end <- append(end,rep(years[i+1],num_mvp_candidates[i]))
}
data$end <- end

data$begin <- as.factor(data$begin)
data$end <- as.factor(data$end)
data$Player_season <- paste0(data$Player_Name,"_",data$begin,"_",data$end)
data <- data %>% filter(Tm != "TOT")

#----------------------------------------------

data2 = data.frame()
for (i in id){
  data2 <- rbind(data2,dbReadTable(con,paste0("vv_MVP_VOTING_",years[i],"_",years[i+1])))
}

num_mvp_candidates <- c()
for (i in id){
  num_mvp_candidates <- append(num_mvp_candidates,nrow(dbReadTable(con,paste0("vv_MVP_VOTING_",years[i],"_",years[i+1]))))
}


data2$begin <- NA
data2$end <- NA

begin <- c()
for (i in id){
  begin <- append(begin,rep(years[i],num_mvp_candidates[i]))
}
data2$begin <- begin

end <- c()
for (i in id){
  end <- append(end,rep(years[i+1],num_mvp_candidates[i]))
}
data2$end <- end
data2$X2P. <- NULL
data2$begin <- as.factor(data2$begin)
data2$end <- as.factor(data2$end)


data2$team_wins <- NA
data2$team_losses <- NA
for (i in 1:nrow(data2)){
  data2[i,"team_wins"] <- as.numeric(unlist(sub("-.*","",data2[i,"Record"])))
  data2[i,"team_losses"] <- as.numeric(unlist(sub(".*-","",data2[i,"Record"])))
}
data2$team_win_percentage <- data2$team_wins/(data2$team_losses+data2$team_wins)
data2$percent_games_played <- data2$G/(data2$team_losses+data2$team_wins)
data2$Player_season <- paste0(data2$Player_Name,"_",data2$begin,"_",data2$end)

output <- data %>% group_by(Player_season) %>% count() %>% arrange(desc(n)) %>% filter(n == 1)
output2 <- data2 %>% group_by(Player_season) %>% count() %>% arrange(desc(n)) %>% filter(n == 1)
data <- data[which(data$Player_season %in% output$Player_season),]
data2 <- data2[which(data2$Player_season %in% output2$Player_season),]

data3 <- merge(data[,c("TRB.PER.36","PTS.PER.36","AST.PER.36","ORtg","DRtg","NetRtg","Player_season")],data2,by = "Player_season")
data3$Share <- ifelse(is.na(data3$Share),0,data3$Share)
data3 <- data3 %>% filter(PTS >= 20 & MP >= 30 & percent_games_played >= 0.75)
data3 <- na.omit(data3)

additional_2019_2020 <- dbReadTable(con,"vv_2019_2020_additional_stats")
additional_2019_2020 <- additional_2019_2020 %>% filter(Tm != "TOT")
mvp_2019_2020 <- dbReadTable(con,"vv_MVP_VOTING_2019_2020")

data_2019_2020 <- merge(additional_2019_2020[,c("Player_team","Tm","TRB.PER.36","AST.PER.36","PTS.PER.36","ORtg","DRtg","NetRtg")],
      mvp_2019_2020[,c("Player_team","Record","PTS","AST","BLK","FT.","G","MP","TRB","PER","TS.","USG.","VORP","WS.48","BPM")],by = "Player_team")

data_2019_2020$team_wins <- NA
data_2019_2020$team_losses <- NA
for (i in 1:nrow(data_2019_2020)){
  data_2019_2020[i,"team_wins"] <- as.numeric(unlist(sub("-.*","",data_2019_2020[i,"Record"])))
  data_2019_2020[i,"team_losses"] <- as.numeric(unlist(sub(".*-","",data_2019_2020[i,"Record"])))
}
data_2019_2020$team_win_percentage <- data_2019_2020$team_wins/(data_2019_2020$team_losses+data_2019_2020$team_wins)
data_2019_2020$percent_games_played <- data_2019_2020$G/(data_2019_2020$team_losses+data_2019_2020$team_wins)

data3$trifecta <- data3$PTS+data3$TRB+data3$AST
data_2019_2020$trifecta <- data_2019_2020$PTS + data_2019_2020$TRB+data_2019_2020$AST 

# data_2019_2020$REBS_ASTS <- data_2019_2020$TRB+data_2019_2020$AST
# data3$REBS_ASTS <- data3$TRB+data3$AST

cols_selected <- c("TRB.PER.36","PTS.PER.36","AST.PER.36","ORtg","DRtg","NetRtg","PTS",
                   "AST","BLK","FT.","TRB","PER","TS.","USG.","VORP","WS.48","BPM",
                  "team_win_percentage","percent_games_played","MP","trifecta")


test <- na.omit(data_2019_2020[,c(cols_selected,"Player_team")])
test <- test %>% filter(PTS >= 20 & MP >= 30 & percent_games_played >= 0.75)
test <- test[,!colnames(test) %in% c("percent_games_played","TRB.PER.36","AST.PER.36","ORtg","FT.","BLK","DRtg","MP","TRB","AST","TS.","NetRtg","PTS","USG.")]

x.train <- data3[,c(cols_selected,"Share")]
x.train <- x.train[,!colnames(x.train) %in% c("percent_games_played","TRB.PER.36","AST.PER.36","ORtg","FT.","BLK","DRtg","MP","TRB","AST","TS.","NetRtg","PTS","USG.")]
#y.train <- data3[,c("Share")]

library(gbm)
set.seed(123)
boost.nba <- gbm(Share~.,data=x.train,distribution= "gaussian",n.trees=5000,interaction.depth=3,shrinkage = 0.1,cv.folds = 4)
mvp_2019_2020_predicted_results <- predict(boost.nba,test,n.trees = 5000)
results <- cbind(test,mvp_2019_2020_predicted_results)
head(results %>% arrange(desc(mvp_2019_2020_predicted_results)),10)


