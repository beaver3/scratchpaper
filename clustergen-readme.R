#' ---
#' title: "Cluster Generation Tool"
#' author: "Valerio Zanetti"
#' date: "June 2019"
#' ---
#' 
setwd("~/code/r")
suppressWarnings(suppressMessages(source("clustergen-1.65.R", echo=FALSE)))

#' Helper functions

#' Function mt() creates monoton increasing list of values from input list  
#' useful too model bare metal resources in scenario with variable demand to get high water mark
N<-20

df<-tibble(demand = jitter(1:N, amount = 2*sqrt(N)), highwatermark=mt(demand), time=1:N)
head(df)
#+ fig.height=5
dd = melt(df, id=c("time"))
ggplot(dd) + geom_line(aes(x=time, y=value, colour=variable), size=1) + scale_colour_ordinal()

#' Function cxyn() connects each elements of x to n elements of y and returns dataframe
#' useful to create network topologies, i.e. spine leaf
df <- cxyn(head(letters,10),1:3,n=2)  
head(df)
#' Check even distribution, expected rowsum = 2 for n=2
t<-table(df)
rowSums(t)
df <- cxyn(head(letters,10),1:3,n=2)  
head(df)
#' Function plotg() plots generated graph
#+ fig.height=5
plotg(df)
#' Check even distribution, expected rowsum = 3 for n=3
df <- cxyn(head(letters,20),1:3,n=3)  
t<-table(df)
rowSums(t)
#' Plot generated graph again
#+ fig.height=5
plotg(df)

#' Create 100 TB cluster with spine leaf topology
c<-cl(usable_pb = 0.1)
head(c)
#' Function slt() creates appropriate spine/leaf topology
t<-slt(c)
head(t)
#' Plot cluster
#+ fig.height=5
plotcl(t)

#' Grow cluster from 10 TB to 10 PB
#+ fig.height=5
p1<-plotcl(slt(cl(usable_pb = 0.01)))
p2<-plotcl(slt(cl(usable_pb = 10)))
grid.newpage()

# Create layout : nrow = 2, ncol = 2
pushViewport(viewport(layout = grid.layout(nrow = 1, ncol = 2)))
define_region <- function(row, col) {
  viewport(layout.pos.row = row, layout.pos.col = col)
} 
print(p1, vp = define_region(row = 1, col = 1))
print(p2, vp = define_region(row = 1, col = 2))

#' Does every node have 2 connections to a leaf? And are the connections to the leafs evenly distributed?
#+ fig.height=5
c<-cl(usable_pb = 10)
#' Cluster specification
dput(c)
lt <- slt(c)
dt<-as.data.table(lt$edges)
n2l <- table(dt[like(from,"node") & like(to,"leaf")])
cs<-colSums(n2l)
rs<-rowSums(n2l)
#' colsums := number of links per leaf
plot(cs, type="s")
#' rowsums := number of links per node (must be 2)
plot(rs, type="s")

#' Generate random walks on graph to simulate workload
g<-graph_from_data_frame(dt)
w<-random_walk(g,start=101,steps=10, mode="all", stuck="return")
m<-as.matrix(w)
m
