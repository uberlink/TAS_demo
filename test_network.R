library(igraph)
library(jsonlite)

#ftag <- "pajek/test.multi.all.daily.2015-09-09"
ftag <- "pajek/test.multi.all.daily.2015-09-10"
#ftag <- "pajek/test.multi.all.all.all"

#read in the network in pajek format
g <- read_graph(gzfile(paste0(ftag,".net.gz")), format = "pajek")

#read in the vertex attributes
vertex_attr <- read.csv(gzfile(paste0(ftag,".vertex_attributes.csv.gz")),header=TRUE)

#add vertex attributes to graph
V(g)$followersCount <- vertex_attr$followersCount
V(g)$friendsCount <- vertex_attr$friendsCount
V(g)$statusesCount <- vertex_attr$statusesCount

#read in the edge attributes
#make sure tweetIds read in as character
edge_attr <- read.csv(gzfile(paste0(ftag,".edge_attributes.csv.gz")),header=TRUE,colClasses="character")

#add edge attributes to graph
E(g)$tweetId <- edge_attr$tweetId
E(g)$edgeType <- edge_attr$edgeType
E(g)$postedTime <- edge_attr$postedTime

#read in the tweet payloads
json_str <- paste(readLines(gzfile(paste0(ftag,".payload.json.gz"))), collapse="")
payload <- fromJSON(json_str)

#add payloads as edge attributes
#head(match(E(g)$tweetId,payload$tweetId))
E(g)$payload <- payload$content[match(E(g)$tweetId,payload$tweetId)]

g

#create subnet of only reply edges
toDelete <- which(E(g)$edgeType!="reply")
g2 <- delete.edges(g,toDelete)

cc <- clusters(g2)             #information on connected components
gc <- induced_subgraph(g2, which(cc$membership == which.max(cc$csize)))    #subnetwork - giant component
V(gc)$label <- V(gc)$id

png("gc_reply.png", width=800, height=700)
plot(gc,vertex.shape="none",edge.width=1.5,edge.curved = .5,edge.arrow.size=0.5,asp=9/16,margin=-0.15)
dev.off()
