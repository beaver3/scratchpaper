# cluster optimization
# V. zanetti/2019

setwd("~/code/r")

library(ggplot2)
library(ggraph)
source("clustergen-1.65.R")

plot_summary <- function (df,to_file = FALSE) {
  #browser()
  df2<-melt(df,id.vars=c("time","fs_cores"), 
            measure.vars = c("usable_cores","usable_pb","cores","snodes","cnodes","shuffle","cidle","core2fab","cost","acost"))
  cluster_spec = paste("TB RAM:",round(min(df$tb_ram)), "-",round(max(df$tb_ram)),
                       " Cores:",min(df$cores),"-",max(df$cores),
                       " PB:",round(min(df$tb_usable)/1024),"-",round(max(df$tb_usable)/1024), 
                       sep="")
  plotname = paste(
    paste(max(df$tb_usable[1]),"TB",max(df$cores),"cores", sep = "_"),
    "png",
    sep = "."
  )
  #browser()
  #scriptname = paste("Cluster generated with",getSrcFilename(cl),sep=" ")
  p<- ggplot(data=df2)+
    geom_point( alpha=0.5,aes(color = factor(fs_cores), x=time,y=value,group=variable),
               position="identity")+
    facet_grid(variable ~ .,scales="free")+xlab("Time") +
    ggtitle(cluster_spec)#+xlim(10,20000)
  
  #  ggplot(data = MYdata, aes(x = farm, y = weight)) + geom_jitter(position = position_jitter(width = 0.3), aes(color = factor(farm)), size = 2.5, alpha = 1) + facet_wrap(~fruit)
  if (to_file) {
    setwd("~/figure")
    ggsave(plotname)
  } else {
    p  
  }
  
}

# utility function to create cluster rampup
ramp_up <- function (timeline = c(1,365,2*365, 3*365), 
                     req_cores = c(500,2000,10000, 10000), 
                     req_pb = c(1,100,150,150), 
                     brownian = TRUE,
                     ...) {
  t <- seq(min(timeline),max(timeline))
  usable_cores <- ceiling(interp1(timeline,req_cores,t, method = "cubic"))
  usable_pb = ceiling(interp1(timeline,req_pb,t,method = "cubic"))
  if (brownian) {
    N<-length(usable_pb)
    usable_cores <- ceiling(usable_cores + br(N, sd=50))
    usable_pb <- ceiling(usable_pb + br(N, sd=0.75))
  }
  df <- cl(time = t, usable_cores = usable_cores, usable_pb =  usable_pb,...)
  df
}

cost_function <- function(x) {
  df  <- ramp_up(fs_cores = x)
  with(df,sum(cost))
}

runs <- seq(1,20)
fscorerange <- seq(4,30)

l<-unlist(lapply(runs,function(x) {
  l1<-unlist(lapply(fscorerange,cost_function))
  fscorerange[which.min(l1)]
}))

# find  fs_cores which yield minimum costs during rampup
minval <- round(mean(l))
paste("Minimal Ramp Up costs reached with ",minval, " fs_cores",sep = "")

df1 <- ramp_up(fs_cores = minval)
#df2 <- ramp_up(fs_cores = minval-10)
df2 <- ramp_up(fs_cores = minval+10)
df <- rbind(df1,df2)
plot_summary(df)


plotcl(slt(df1))
