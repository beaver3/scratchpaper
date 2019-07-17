# generate cluster topology as data table
# (C) V.Zanetti/T-Systems/2019

# v1.0  start
# v1.3  create cluster topology

rm(list = ls())
library(data.table)
library(tibble)
library(pracma)
library(igraph)
library(Ryacas)
library(extrafont)
library(ggraph)
library(graphlayouts)
library(reshape2)
library(grid)
#loadfonts()

# create monotonic function
mt <- function(y) {
  if (length(y)<2) {
    return(y) 
  }
  else {
    r<-y
    for (i in seq(2,length(y))) {
      if (r[i-1]>r[i]) r[i]=r[i-1]
    }
    r  
  }
  
}

# brownian motion
br <- function(N, mean=0, sd = 1) {
  dis = rnorm(N, mean = mean, sd = sd)
  cumsum(dis)
}


# points on a circle
# generate n x/y points on a circle with center x0,y0
poc <- function(n=10,r=1.0,from=0,to=2*pi,x0=0,y0=0) {
  delta = abs(from-to)
  angles <- head(seq(from,to, by=delta/n),n)
  result = data.table(
    x = r * cos(angles)+x0,
    y = r * sin(angles)+y0
  )
  result
}

# points on a line
# generate n x/y points on a line with space in between and center x0,y0
pol <- function(n=10,d=1.0,x0=0,y0=0) {
  result = data.table(
    x = seq(1,n)*d+x0-(n*d)/2,
    y = y0
  )
  result
}


# points on a square
# generate n x/y points inside a square with side length s and center x0,y0
poq <- function(n=10,s=1.0,x0=0,y0=0) {
  dim <- ceiling(sqrt(n))
  x <- seq(0,s,length.out = dim)
  g<-expand.grid(x,x)
  colnames(g) <- c("x","y")
  result = data.table(
    x = head(g$x,n)+x0,
    y = head(g$y,n)+y0
  )
  result
}


# cluster creation
cl <- function (
  time                 = 1,       # one month
  core_gigabit         = 8*2,     # 2 gbyte/s troughput per core
  usable_cores         = 4000,    # what do we need (cores)
  usable_pb            = 100,     # what do we need (capacity)
  repl_factor          = 1.4,     # replication factor, 1.4 => erasure code (10+4)
  fabric_nic_speed     = 100,     # gbit nic speed
  cnode_nic_speed      = 2*25,    # gbit nic speed per cnode
  cnodes_per_rack      = 16,
  snode_nic_speed      = 2*25,    # gbit nic speed per snode
  snodes_per_rack      = 16,
  # setting snode_cores = fs_cores disaggregates storage, 
  # with snode_cores = fs_cores  no cores will be left for compute, usable cores must be provided by cnodes 
  snode_cores          = 42,      # available cores per snode (some of it reserved via fs_cores)
  snode_ram            = 384,     # available ram per node
  snode_disks          = 24,      # direct attached disks per snode
  fs_cores             = 6,
  cnode_cores          = 36,      # available cores per cnode
  cnode_ram            = 384,     # available cores per cnode
  disk_tb              = 14,
  snode_tb             = snode_disks*disk_tb, # tb raw per storage node
  spine_slots          =  4,      # slots per spine switch for line cards: 4 / 8 / 16 
  spine_to_spine       = 12,      # number of links between spines
  leaf_to_spine        =  6,      # uplinks from leaf to spine
  border_nics          =  6,      # 6 nics per spine reserverd for border routing
  leaf_ports           = 48,      # available leaf switch ports, number of 100 Gbit ports
  node_to_leaf         = 32,      # max nodes per leaf
  leaf_to_leaf         = leaf_ports-node_to_leaf       # ports to connect to leafs
) 
{
  
  # get parameters from function call to pass to generated data frame
  incall <- mget(ls())
  
  # check consistency
  stopifnot(fs_cores < min(snode_cores))
  #stopifnot(avg_job_size < min(usable_cores))
  #browser()
  cdef <- tibble (
    # cluster sizing 
    snodes               = mt(ceiling(usable_pb*1024*repl_factor/snode_tb)), # number of storage nodes
    scores               = snodes*(snode_cores-fs_cores),  # usable storage cores
    remaining_cores      = unlist(lapply((usable_cores-scores),function(x) {if (x<0) 0 else x})),
    cnodes               = mt(ceiling(remaining_cores/cnode_cores)), # number of compute nodes in cluster
    ccores               = cnodes*cnode_cores,
    nodes                = cnodes + snodes,
    tb_raw               = snodes*snode_tb,
    tb_usable            = snodes*snode_tb/repl_factor,
    cores                = scores+ccores,
    tb_ram               = (cnode_ram*cnodes+snode_ram*snodes)/1024,
    spine_nics           = 36*spine_slots-border_nics,    # 100gbit nics per spine
    cracks               = ceiling(cnodes/cnodes_per_rack),
    sracks               = ceiling(snodes/snodes_per_rack),
    racks                = sracks+cracks,
    leafs                = 2*racks,  # 2 leaf switches per racks
    uplinks              = leaf_to_spine * leafs,
    s                    = (nthroot(spine_to_spine^2 - -4 * ((spine_nics - spine_to_spine) * uplinks), 2) - spine_to_spine)/(2 * (spine_nics - spine_to_spine)),
    spines               = unlist(lapply(ceiling(s), function(x) {if (x<2) 2 else x})),
    interconnects        = (spines-1)*spine_to_spine,
    fabric_gbit          = interconnects*fabric_nic_speed,
    uplink_gbit          = leafs*leaf_to_spine*fabric_nic_speed,
    border_gbit          = spines*border_nics*fabric_nic_speed,
    leaf_gbit            = snodes*snode_nic_speed+cnodes*cnode_nic_speed,
    
    # the following kpis need to be minimized
    # idle cores: cores not used for computation
    cidle                = usable_cores/(snodes*snode_cores+cnodes*cnode_cores),
    # core  to fabric oversubscription
    core2fab       = cores*core_gigabit/fabric_gbit,
    # shuffle ratio 
    shuffle              = cnodes/nodes, #*choose(cores,avg_job_size)/tcores,
    cost                  = sqrt(shuffle^2+cidle^2+core2fab^2),
    acost                = cumsum(cost)
  )
  
  # add input parameters to table
  cdef2 <- cbind(cdef,incall)
  cnm <- colnames(cdef2)
  dt<-as.data.table(cdef2)
  colnames(dt) <- colnames(cdef2)
  rownames(dt) <- NULL
  dt
}


# connects all elements of x to n distinct elements of y 
# connect nodes to a pair of leafs, with unique leafs per pair, 
# use: c( cxyn(x,y=leaf_odd,n=1), cxyn(x,y=lead_even, n=1)) 
cxyn <- function(x,y,n=2,capa=1) {
  stopifnot(length(x)>0)
  stopifnot(length(y)>=n)
  #browser()
  # endpoint tuples
  ep <- combn(y,n)
  lep <- dim(ep)[2] # number of possible distinct endpoint combinations after comb
  # randomly choose indices
  ix<-mod(sample(seq(1,length(x))),lep)
  #ix<-mod(seq(1,length(x)),lep)
  if (min(ix)==0) ix <- ix+1
  el <- lapply(ix,function(p){ep[,p]})
  result <- tibble(from = rep(x,each=n),
                   to = unlist(el),
                   capacity = capa
  )
  result
}

# test cxyn
t_cxyn <- function(x,y,n=2) {
  t<-table(cxyn(x,y,n))
  plot(rowSums(t), type="s")
  plot(colSums(t), type="s")
}


# take a cluster generated with function cl and return ftt as dt
# ftt => fat tree topology
# test with: plotcl(ft(cl(usable_pb=0.1, usable_cores = 2000, node_to_leaf = 16)), decorate = FALSE)
ft <- function(dt) {
  dt <- tail(dt,1)
  #browser()
  # calculate leaf switches for full fat tree
  dt$leafs <- ceiling(dt$nodes/dt$node_to_leaf)
  dt$spines <- ceiling(dt$leafs/2)
  # connect  switching topology
  lf1 <- paste("leafA_",seq(1,dt$leafs),sep="")
  sp1 <- paste("spineA_",seq(1,dt$spines),sep="")
  lf2 <- paste("leafB_",seq(1,dt$leafs),sep="")
  sp2 <- paste("spineB_",seq(1,dt$spines),sep="")
  if (dt$cnodes>0) { 
    cn <- paste("cnode_",seq(1,dt$cnodes),sep="")
  } else { cn <- NULL }
  
  if (dt$snodes>0) {
    sn <- paste("snode_",seq(1,dt$snodes),sep="")
  } else { sn <- NULL }
  nodes<-c(cn,sn)
  #browser()
  # leaf_ports           = 48,      # available leaf switch ports, number of 100 Gbit ports
  # node_to_leaf         = 32,      # max nodes per leaf
  # leaf_to_leaf         = leaf_ports-node_to_leaf       # ports to connect to leafs
  
  # create nodes to leafs1
  e1<-as.data.table(cxyn(nodes,lf1,1,capa = 1))
  e2<-as.data.table(cxyn(nodes,lf2,1,capa = 1))
  e1[like(from,"cnode")]$capacity=dt$cnode_nic_speed
  e2[like(from,"snode")]$capacity=dt$snode_nic_speed
  
  # create leaf to spine
  e3<-cxyn(lf1,sp1,min(dt$spines,dt$leaf_to_leaf), capa = dt$leaf_to_spine*dt$fabric_nic_speed)
  e4<-cxyn(lf2,sp2,min(dt$spines,dt$leaf_to_leaf), capa = dt$leaf_to_spine*dt$fabric_nic_speed)
  
  e <- rbind(e1,e2,e3,e4)
  v <- tibble( name = unique(c(e$from, e$to)), x= 0.0, y=0.0, type="node")
  # layout stuff manually on 2 half circles (inner circle = leafs)
  v<-unique(as.data.table(v))
  
  radius <- 1.0
  xyl1 <- pol(n=dt$leafs,d = radius*2/dt$leafs,y0=-2.0, x0=-0.25*radius)
  xys1 <- pol(n=dt$spines,d = radius*2/dt$spines,y0=-3.0, x0=-0.25*radius)
  xyl2 <- pol(n=dt$leafs,d = radius*2/dt$leafs,y0=2.0, x0=-0.25*radius)
  xys2 <- pol(n=dt$spines,d = radius*2/dt$spines,y0=3.0, x0=-0.25*radius)
  xyn <- poq(n=dt$nodes,s = 2*radius,x0 = -radius, y0=-radius)
  #browser()
  v[like(name,"leafA")]$x=xyl1$x
  v[like(name,"leafA")]$y=xyl1$y
  v[like(name,"leafB")]$x=xyl2$x
  v[like(name,"leafB")]$y=xyl2$y
  v[like(name,"spineA")]$x=xys1$x
  v[like(name,"spineA")]$y=xys1$y
  v[like(name,"spineB")]$x=xys2$x
  v[like(name,"spineB")]$y=xys2$y
  v[like(name,"leaf")]$type="leaf switches"
  v[like(name,"spine")]$type="spine switches"
  v[like(name,"cnode")]$type="compute node"
  v[like(name,"snode")]$type="storage node"
  v[like(name,"node")]$x=xyn$x
  v[like(name,"node")]$y=xyn$y
  return (list(edges=e, vertices=v))
}

# take a cluster generated with function cl and returns spine leaf topology as dt
slt <- function(dt) {
  # take last row if dt is a list of clusters => last step in time series
  dt <- tail(dt,1)
  #add switching topology
  sp <- paste("spine_",seq(1,dt$spines),sep="")
  lf_odd <- paste("leaf_",seq(1,dt$leafs,by=2),sep="")
  lf_even <- paste("leaf_",seq(1,dt$leafs,by=2)+1,sep="")
  lf <- c(lf_odd,lf_even)
  
  # endpoints for leafs (pick 2 spines per leaf)
  g1 <- cxyn(x = lf, y=sp, n=2, capa = dt$leaf_to_spine*dt$fabric_nic_speed)
  
  # full mesh on spine layer
  full_mesh <- matrix(data = combs(sp,2), ncol = 2)
  g2 <- tibble(from = full_mesh[,1],
               to = full_mesh[,2],
               capacity = dt$spine_to_spine*dt$fabric_nic_speed
  )
  
  # add nodes to leafs
  if (dt$cnodes>0) { 
    cn <- paste("cnode_",seq(1,dt$cnodes),sep="")
  } else { cn <- NULL }
  
  if (dt$snodes>0) {
    sn <- paste("snode_",seq(1,dt$snodes),sep="")
  } else { sn <- NULL }
  
  # endpoints for nodes (pick 2 leafs per node)
  g3 <- cxyn(x = c(sn,cn), y=lf_odd, n=1)
  g4 <- cxyn(x = c(sn,cn), y=lf_even, n=1)
  node2leafedges <- as.data.table(rbind(g3,g4))
  node2leafedges[like(from,"snode")]$capacity <- dt$snode_nic_speed
  node2leafedges[like(from,"cnode")]$capacity <- dt$cnode_nic_speed
  
  # final edge list
  e <- rbind(g1,g2,node2leafedges)
  v <- tibble( name = unique(c(e$from, e$to)), x= 0.0, y=0.0, type="node")
  
  # layout stuff manually on circles (inner circle = leaf)
  v<-unique(as.data.table(v))
  xys <- poc(n=dt$spines,r=1.0)
  xyl <- poc(n=dt$leafs,r=2.0)
  xyn <- poc(n=dt$nodes,r=4.0)
  #browser()
  v[like(name,"leaf")]$x=xyl$x
  v[like(name,"leaf")]$y=xyl$y
  v[like(name,"leaf")]$type="leaf switch"
  v[like(name,"spine")]$x=xys$x
  v[like(name,"spine")]$y=xys$y
  v[like(name,"spine")]$type="spine switch"
  v[like(name,"cnode")]$type="compute node"
  v[like(name,"snode")]$type="storage node"
  v[like(name,"node")]$x=xyn$x
  v[like(name,"node")]$y=xyn$y
  return (list(edges=e, vertices=v))
}


# create simple graph plot
# sample call: plotg(cxyn(head(letters,10),1:2))
plotg <- function(edges, layout = "circle", labels = F) {
  g <- graph_from_data_frame(edges)
  l <- ggraph(g,layout = layout)+
    geom_edge_link(alpha = 0.1) + 
    geom_node_point(aes(size=degree(g)), alpha = 0.1, show.legend = T) +
    theme_graph()
  if (labels)
    l <- l+geom_node_text(aes(label = V(g)$name),show.legend = F,size=3,check_overlap = TRUE, repel = TRUE) 
  l
}

# takes cluster data table, creates graph and plots cluster
plotcl <- function(dt, decorate = TRUE) {
  g  <- graph_from_data_frame(dt$edges)
  node_type <- dt$vertices$type
  if (decorate) {
  ggraph(g,layout = 'manual', node.positions = dt$vertices[,c("x","y")])+
    geom_edge_link(aes(width = capacity), alpha = 0.1) + 
    scale_edge_width(range = c(0.05, 3)) +
    #geom_node_text(aes(label = name), alpha=0.8, size=3,check_overlap = TRUE, repel = TRUE) +
    geom_node_point(
      aes(size=degree(g),color=node_type), 
      show.legend = T,
      alpha = 1.0) +
    labs(edge_width = "Gbit/s") +
    theme_graph()
  } else {
    ggraph(g,layout = 'manual', node.positions = dt$vertices[,c("x","y")])+
      geom_edge_link(alpha = 0.1) + 
      geom_node_point(aes(size=degree(g)), alpha = 0.1, show.legend = T) +
      theme_graph()
  }
}

# not run test
# plotcl(slt(cl()))
# table(slt(cl())$edges$to) to verify even distribution of nodes to leafs