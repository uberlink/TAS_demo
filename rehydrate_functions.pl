#MIT License

#Copyright (c) 2017 Robert Ackland

#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

use diagnostics;
use warnings;
use strict;

use Data::Dumper;
use Storable qw(fd_retrieve store_fd);

use Date::Calc qw(:all);
use XML::Simple;
use XML::DOM;
use IO::Zlib;
use PerlIO::gzip;
use JSON;
use Text::CSV;

our %tweets = ();
our %nodes = ();
our %nodesFull = ();
our %edges = ();
our %handles = ();


our $tag;
our $ftag;
our $minDate;
our @periods = ();
our @target = ();
our %important;

my %h = ();              

our @attr_gen = ();
our @attr = ();
our @desc = ();

#following are attributes provided by Twitter - they are all stocks
our @attr_twitter = ('followersCount', 'friendsCount', 'statusesCount' );

sub constructHash_lineParse_new{

    my $fn = shift;

    print "Started constructHash_lineParse_new(): $fn\n";
    my @nodeAttrKeep = ( "friendsCount", "followersCount", "preferredUsername", "statusesCount", "link", "displayName" );
    my %nodeAttrKeep = ();
    foreach my $k ( @nodeAttrKeep ){ $nodeAttrKeep{$k} = 1 };

    my $fh = new IO::Zlib;
    $fh->open("data_from_uberlink/$fn.graphml.gz", "rb");
    my %tweets = ();
    my %nodes = ();
    my %edges = ();
    my $nodeId = undef;
    my $edgeId = undef;
    my $j = 0;
    while (<$fh>){
	chomp;
	#print $_."\n";
	#tweets
	if ( /^\s+<data key="(g_\d+)">(.+)<\/data>$/ ){
	    #print $_."\n";
	    my $key = $1;
	    my $tweet = $2;
	    $key =~ s/g_//g;
	    #print "$tweet\n";
	    $tweet =~ s/&quot;/"/g;
	    #print "$tweet\n";
	    #my $tt = '{ "Id": 1, "Name": "Coke" }';
	    my $myjson_r = decode_json( $tweet );
	    #print Dumper($myjson_r)."\n";
	    $tweets{$key} = $myjson_r;
	}

	#nodes
	if ( /^\s+<node id="(n\d+)">$/ ){       #start of node element
	    $nodeId = $1;
	}
	if ( $nodeId ){
	    #print "in node element: $_\n";
	    if ( /^\s+<data key="(.+)">(.+)<\/data>$/ ){      
		#print "$1, $2\n";
		my $k = $1;
		my $v = $2;

		#these are graph-theoretic attributes in each network (network covering entire period)
		if ( $k eq "v_reply" || $k eq "v_retweet" || $k eq "v_mention" ){
		    $v =~ s/&quot;/"/g;
		    $v = decode_json( $v );

		    foreach my $i ( keys %{$v} ){
			#if start picking up more UL-generated attributes, will need to change code in populateHandlesHash()
			delete $v->{$i} if ( $i !~ /degree|indegree|outdegree/ ); 
		    }
		    my $k2 = $k;
		    $k2 =~ s/^v_//;
		    $nodes{$nodeId}{'network'}{$k2} = $v;
		    #print Dumper(\%nodes)."\n";
		    #exit;
		}

		#profile data - stored as an array of hashes.  First entry in the array is a complete snapshot of profile as at a particlar
                #date.  Thereafter, the entries are hashes with the profile attributes which changed, at each date of subsequent activity.
		if ( $k eq "v_profile" ){
		    $v =~ s/&quot;/"/g;
		    $v = decode_json( $v );
		    #print Dumper($v)."\n";

		    foreach my $t ( @{$v} ){
			#Jan 2017: now including in network actors who may not have tweeted on target, hence no profile data
			my $collDate = ( exists $t->{'collectionDate'} ) ? $t->{'collectionDate'} : $minDate."T00:00:00.000Z";
			foreach my $k2 ( keys %{$t->{'data'}} ){
			    #print "$k2\n";
			    if ( $nodeAttrKeep{$k2} ){
				$nodes{$nodeId}{$collDate}{$k2} = $t->{'data'}->{$k2};
				$nodes{$nodeId}{$collDate}{$k2."2"} = lc($t->{'data'}->{$k2}) if ( $k2 eq "preferredUsername" );
			    }
			}
			#last;             #just collecting first snapshot (for now)
		    }
		}
	    }
	}
	if ( /^\s+<\/node>$/ ){       #end of node element
	    $nodeId = undef;
	}

	#edges
	if ( /^\s+<edge source="(n\d+)" target="(n\d+)">$/ ){       #start of edge element
	    $edgeId = "$1,$2";
	}

	if ( $edgeId ){
	    #print "in edge element: $_\n";
	    if ( /^\s+<data key="(.+)">(.+)<\/data>$/ ){      
		#print "$1, $2\n";
		my $k = $1;
		my $v = $2;
		
		if ( $k eq "e_content" ){
		    $v =~ s/&quot;/"/g;
		    $v = decode_json( $v );
		    my @t = split/,/, $edgeId;
		    $edges{$t[0]}{$t[1]} = $v;
		}
	    }
	}
	if ( /^\s+<\/edge>$/ ){       #end of edge element
	    $edgeId = undef;
	}
	print "finished parsing line -$j-\n" if ( $j%5000==0);
	$j++

    }
    close $fh;
    # print Dumper(\%tweets)."\n";
    # print Dumper(\%nodes)."\n";
    # print Dumper(\%edges)."\n";

    print "Started dumping hashes to disc\n";
    system("mkdir -p hashes");             #create dir if need to
    open(my $F_out, ">:gzip", "hashes/$fn.tweets_hash.dat.gz") or die $!;
    store_fd(\%tweets, $F_out);
    close $F_out;
    open($F_out, ">:gzip", "hashes/$fn.nodes_hash.dat.gz") or die $!;
    store_fd(\%nodes, $F_out);
    close $F_out;
    open($F_out, ">:gzip", "hashes/$fn.edges_hash.dat.gz") or die $!;
    store_fd(\%edges, $F_out);
    close $F_out;

    print "Finished constructHash_lineParse_new()\n";

}

sub getHash2{

    #this version uses storable - gzipped by default
    my ( $name ) = @_;
    my $start_run = time();
    print "Getting $name hash\n";

    #note: storable didn't work with IO::Zlib (documented on web too), so I used PerlIO::gzip
    open(my $fh, "<:gzip", "$name.dat.gz") or die $!;
    my $href = fd_retrieve($fh);
    close $fh;
    my $end_run = time();
    my $run_time = $end_run - $start_run;
    print "Getting hash took: $run_time seconds\n";
    return $href;
}

sub networks{

    my ( $match, $seeds_full, $freq_i, $edgetype, $node_attr_client_r, $nodes2_r, $ulid_to_username_r, $printPayload ) = @_;
    my %node_attr_client = %{$node_attr_client_r};
    my %nodes2 = %{$nodes2_r};
    my %ulid_to_username = %{$ulid_to_username_r};
    #print "ulid_to_username: ".Dumper(\%ulid_to_username)."\n";
    #exit;

    print "started networks(): $edgetype, $freq_i\n";
    my @edgetype_arr = ();
    if ( $edgetype eq "multi" ){
	push @edgetype_arr, 'mention';
	push @edgetype_arr, 'reply';
	push @edgetype_arr, 'retweet';
	push @edgetype_arr, 'self-loop';
    }else{
	push @edgetype_arr, $edgetype;	
    }
    #print Dumper(\@edgetype_arr)."\n";
    #exit;

    my %handle_to_id = ();
    foreach my $i ( keys %handles ){
	$handle_to_id{$handles{$i}{$match}} = $i;
    }
    #print Dumper(\%handles)."\n";
    #print Dumper(\%handle_to_id)."\n";
    #exit;

    my %edges2 = ();
    my %payload = ();

    my %node_attr_ul = (  'statusesCount' => 'double', 'preferredUsername2' => 'string', 'followersCount' => 'double', 'friendsCount' => 'double' );

    #The following loop over periods of data collection, constructs two hashes: 
    #  %nodes2 is the attributes of actors, at each time period e.g. monthly, weekly
    #  %edges2 is the edges created in each time period
    #  While %nodes and %edges have ulid id numbers as keys, remember that these are not constant across periods
    #  So %nodes2 and %edges2 use the usernames as keys.  Need to do this because it is possible that e.g. tweets for a given week
    #  come from two different periods of data and hence same actor has different ulid.  So need to use usernames.
    #  But note: the following (and all my code) assumes that usernames are not changing across periods i.e. if person changes usernames, we can't track this at present

    foreach my $p ( @periods ){
	print "Working on period: $p\n";
	my $fn = $ftag.$p;

	my %edges = %{getHash2("hashes/$fn.edges_hash", "gz")};
	#print "edges: ".Dumper(\%edges)."\n";
	#exit;
	my %tweets = %{getHash2("hashes/$fn.tweets_hash", "gz")};
	#print Dumper(\%tweets)."\n";
	#exit;

	#Set so edge from i to j will only exist if:
	# i authors tweet that mentions/replies to j
	# i retweets j
	# (therefore will ignore edge if i retweets k who authors tweet that mention/replies to j)
	# (will also ignore mention edge from i to j, which arises from i retweeting j)
	
	my $v = 1;
	foreach my $i ( keys %edges ){

	    #print "i: $i\n";
	    #print Dumper($edges{$i})."\n" if ( $i eq "n45409" );
	    
	    #need hash of retweets from this user
	    #this is used for two purposes:
	    #(1) reply/mention edges only have originalID.  Note that what appears as reply/mention edge may have in fact resulted from retweet.  If e.g. mention  edge resulted from original tweet (not retweet), then originalID will be in %tweets and we can get e.g. postedTime from there.  Also originalID will not match id of tweet being retweeted by this actor.  But if e.g. mention edge resulted from retweet, then originalID will be the id of the tweet being retweeted by this actor, retweetID will be the id of the retweet activity, and retweetDate will be the date of the retweet activity
	    #(2) we will also use this to ignore indirect edges (i.e. i retweets j who mentioned k, then won't be edge from i to k)
	    #NOTE: something similar is used in printTweetPayloads(), but not exactly the same....

	    #Illustration of %edges data structure (advocacy, first month):
#         'n89515' => {
#                       'n80737' => {
#                                     'retweet' => [
#                                                    {
#                                                      'retweetDate' => '2015-10-07T21:14:54.000Z',     TIMESTAMP OF n89515's RETWEET (also in %tweets)
#                                                      'retweetID' => '651868325409103872',             ID OF n89515's RETWEET
#                                                      'originalID' => '651656606489440257'             ID OF TWEET BEING RETWEETED BY n89515
#                                                    }
#                                                  ],
#                                     'mention' => [
#                                                    {
#                                                      'originalID' => '651656606489440257'              ID OF TWEET BEING RETWEETED BY n89515
#                                                    }
#                                                  ]
#                                   },
#                       'n75724' => {
#                                      'mention' => [
#                                                     {
#                                                       'originalID' => '651656606489440257'             ID OF TWEET BEING RETWEETED BY n89515
#                                                     }
#                                                   ]
#                                    }
#                      },

#from ulid_to_username:
#'n89515' -> 'katxhood'
#'n80737' => '5sos'
#'n75724' => 'aria_official'

#Following is the relevant entries from %tweets:
#          '651656606489440257' => {
#                                    'postedTime' => '2015-10-07T07:13:36.000Z',
#                                    'content' => 'Insanity! Could not be happier on this cold English morning @ARIA_Official nom&apos;s for "best live act" and "best group" we are stoked!!! Xx',
#                                    'retweets' => [
#                                                    [
#                                                      '651733676221431808',
#                                                      '2015-10-07T12:19:51.000Z'
#                                                    ],
#					....MANY REMOVED....
#                                                    [
#                                                      '651868325409103872',
#                                                      '2015-10-07T21:14:54.000Z'
#                                                    ],

#Interpretation: n89515/katxhood appears to have mentioned both n80737/5sos and n75724/aria_official in a tweet.  
#But because originalID of mention tie matches originalID of a retweet, we know that these two mention edges have come from a retweet, not an original tweet
#Furthermore, n80737 is the person being retweeted, so that is in fact a retweet edge, rather than a mention edge
#Also, given we are ignoring indirect edges, then we drop the tie to n75724 because that has come about just because n89515 has retweeted a tweet
#authored by n80737, where n75724 was mentioned

	    my %retweets = ();
	    foreach my $j ( keys %{$edges{$i}} ){
		foreach my $t ( @{$edges{$i}{$j}{'retweet'}} ){
		    #retweetID - id of the retweet activity, originalID - id of tweet being retweeted, retweetDate - date of retweet activity
		    $retweets{$t->{'originalID'}}{'retweetDate'} = $t->{'retweetDate'};           
		}
	    }
	    # if ( $nodes{$i}{'preferredUsername2'} eq "nrl" ){
	    #     print Dumper(\%retweets);
	    #     exit;
	    # }
	    foreach my $j ( keys %{$edges{$i}} ){
		#next if ( $i eq $j );                                   #Don't want loops.  New 18Oct.  Check impact
		foreach my $edgetype_i ( @edgetype_arr ){
		    if ( exists $edges{$i}{$j}{$edgetype_i} ){
			foreach my $t ( @{$edges{$i}{$j}{$edgetype_i}} ){
			    my $haveEdge = 0;
			    my %ht = ();
			    my $postedTime;
			    if ( $edgetype_i ne "retweet" ){         #check is only made for mentions/replies (unlikely would be reply, but check anyway...)
				
				#check to see if id of this activity matches id of tweet that has been retweeted by this actor
				#if it does, then we don't use it, since will result in indirect edge...
				if ( not exists $retweets{$t->{'originalID'}} ){     #this is edge from original tweet (not a retweet)
				    $postedTime = $tweets{$t->{'originalID'}}{'postedTime'};
				    $haveEdge = 1;
				    $payload{$t->{'originalID'}} = $tweets{$t->{'originalID'}}{'content'};
				    #$payload{$t->{'originalID'}} = 'x';
				}
			    }else{           #edgetype=retweet
				if ( not exists $retweets{$t->{'originalID'}} ){     #this is edge from tweet
				    die "didn't expect to get here...";
				    $postedTime = $tweets{$t->{'originalID'}}{'postedTime'};
				}else{                                               #this is edge from retweet
				    $postedTime = $retweets{$t->{'originalID'}}{'retweetDate'};     #when the retweet activity occurred
				    $payload{$t->{'originalID'}} = $tweets{$t->{'originalID'}}{'content'};
				    #$payload{$t->{'originalID'}} = 'x';
				}
				$haveEdge = 1;
			    }
			    
			    #remember: if mention/reply was from original tweet, then originalID is the id of this original tweet
			    #          if we have a retweet, then originalID is the id of the tweet being retweeted
			    if ( $haveEdge && inRange($postedTime, $minDate, 'edge') ){
				my %freq = %{findPeriods2($postedTime, $freq_i)};
				my $u_i = $ulid_to_username{$p}{$i};
				my $u_j = $ulid_to_username{$p}{$j};
				print "u_i: $u_i, u_j: $u_j\n" if ( $postedTime eq "2016-10-08T14:03:25.000Z" ); 
				foreach my $f ( keys %freq ){
				    $edges2{$f}{$freq{$f}}{$u_i}{$u_j}{$t->{'originalID'}}{'postedTime'} = $postedTime;
				    $edges2{$f}{$freq{$f}}{$u_i}{$u_j}{$t->{'originalID'}}{'edgeType'} = $edgetype_i;
				    #$edges2{$freq{$f}}{$u_i}{$u_j}{$t->{'originalID'}}{'tieWeight'} = 1;
				}	
			    }
			}		
		    }
		}
	    }
	    print "edges: finished working on node $v\n" if ( $v%1000 == 0 );
	    $v++;
	}
	#print "Finished working on %edges\n";
	#exit;

    }

    #Now create new id number for inclusion in the graphml files
    #global id across all time periods "globalid"

    my @freq = ($freq_i,'all');
    
    #globalid
    #iterate over both edges2 and nodes2 to make sure pick them all up
    #only need to iterate over time freq 'all' since all actors in other months will be in 'all'
    my %h_t = ();           #complete list of usernames
    foreach my $i ( keys %{$edges2{'all'}{'all'}} ){
	$h_t{$i} = 1;
	foreach my $j ( keys %{$edges2{'all'}{'all'}{$i}} ){
	    $h_t{$j} = 1;
	}
    }
    foreach my $i ( keys %{$nodes2{'all'}{'all'}} ){
	$h_t{$i} = 1;
    }
    my %username_to_globalid = ();
    my $t = 1;
    foreach my $i ( sort { $a cmp $b } keys %h_t ){
	$username_to_globalid{$i} = "idx".$t;              #"idx" pre-prended to minimise chance of globalid clashing with username
	$t++;
    }
    #print "h_t: ".Dumper(\%h_t)."\n";
    #print "username_to_globalid: ".Dumper(\%username_to_globalid)."\n";
    #exit;

    #check don't have instance of globalid equal to existing username
    #this can mess with the key deletion later on
    foreach my $i ( keys %username_to_globalid ){
        die "problem: global id $username_to_globalid{$i} matches existing username\n" if ( exists $username_to_globalid{$username_to_globalid{$i}});	  
    }

    #print "edges2: ".Dumper(\%edges2)."\n";

    #now replace the keys in %nodes2 and %edges2 with the new id numbers
    foreach my $freq ( @freq ){
        #print "freq: $freq\n";
	foreach my $f ( keys %{$nodes2{$freq}} ){       #sort not necessary
	    foreach my $i ( keys %{$nodes2{$freq}{$f}} ){
		$nodes2{$freq}{$f}{$username_to_globalid{$i}} = delete $nodes2{$freq}{$f}{$i};
		$nodes2{$freq}{$f}{$username_to_globalid{$i}}{'preferredUsername2'} = $i;         #add this in as attribute, since no longer key
	    }
	}
	foreach my $f ( keys %{$edges2{$freq}} ){       #sort not necessary
            #print "\tf: $f\n";
            #my @arr = keys %{$edges2{$freq}{$f}};
	    #print Dumper(\@arr)."\n";
	    foreach my $i ( keys %{$edges2{$freq}{$f}} ){
                #print "\t\ti: $i\n" if ( $i eq 'alderleymel' );
		foreach my $j ( keys %{$edges2{$freq}{$f}{$i}} ){
                    #print "\t\t\tj: $j\n" if ( $i eq 'alderleymel' && $j eq 'saragreenwell' );
                    #die "xxx: $j" if ( not exists $username_to_globalid{$j} );
                    if ( not exists $username_to_globalid{$j} ){
                        print "$freq, $f, $i, $j\n"; 
                        print Dumper($edges2{$freq}{$f}{$i})."\n";
                        print "problem...\n";
			die "xxx";
                    }
		    $edges2{$freq}{$f}{$i}{$username_to_globalid{$j}} = delete $edges2{$freq}{$f}{$i}{$j};
		}
		$edges2{$freq}{$f}{$username_to_globalid{$i}} = delete $edges2{$freq}{$f}{$i};
	    }
	}
    }
    #print "edges2: ".Dumper(\%edges2)."\n";
    #print "nodes2: ".Dumper(\%nodes2)."\n";
    #exit;

    #now, strip the "idx" from globalid because 
    #this is inefficient double handling...maybe revisit sometime
    foreach my $i ( keys %username_to_globalid ){
       my $g_2 = $username_to_globalid{$i};
       $g_2 =~ s/^idx//;
       $username_to_globalid{$i} = $g_2;
    }
    #print "username_to_globalid: ".Dumper(\%username_to_globalid)."\n";
    #exit;
    foreach my $freq ( @freq ){
        foreach my $f ( keys %{$nodes2{$freq}} ){       #sort not necessary
            foreach my $i ( keys %{$nodes2{$freq}{$f}} ){
                my $i_2 = $i;
                $i_2 =~ s/^idx//;
                $nodes2{$freq}{$f}{$i_2} = delete $nodes2{$freq}{$f}{$i};
            }
        }
        foreach my $f ( keys %{$edges2{$freq}} ){       #sort not necessary
            foreach my $i ( keys %{$edges2{$freq}{$f}} ){
                my $i_2 = $i;
                $i_2 =~ s/^idx//;
                foreach my $j ( keys %{$edges2{$freq}{$f}{$i}} ){
                    my $j_2 = $j;
                    $j_2 =~ s/^idx//;
                    $edges2{$freq}{$f}{$i}{$j_2} = delete $edges2{$freq}{$f}{$i}{$j};
                }
                $edges2{$freq}{$f}{$i_2} = delete $edges2{$freq}{$f}{$i};
            }
        }
    }
    #print "edges2: ".Dumper(\%edges2)."\n";
    #exit;

    #Some things we need to do before printing networks
    #(1) check that every node in edges2 also present in nodes2, and vice-versa otherwise will get unequal adjacency matrixes across periods
    #use "audit" hashes from above (note: keys are usernames)

    #better way to find this???
    my %attr_keys_full = ();
    foreach my $i ( keys %{$nodes2{'all'}{'all'}} ){
	foreach my $k ( keys %{$nodes2{'all'}{'all'}{$i}} ){
	    $attr_keys_full{$k} = 1;
	}
	last;
    }
    #print "attr_keys_full: ".Dumper(\%attr_keys_full)."\n";
    #print Dumper(\%handles)."\n";
    #print Dumper(\%handle_to_id)."\n";
    #exit;

    #can optimise this??
    foreach my $i ( keys %h_t ){
    	foreach my $freq ( @freq ){
    	    foreach my $f ( keys %{$nodes2{$freq}} ){
    		if ( not exists $nodes2{$freq}{$f}{$username_to_globalid{$i}} ){
    		    #print "-$i- missing from nodes2 for -$freq:$f-\n";
    		    foreach my $k ( keys %attr_keys_full ){
    			if ( $k =~ /^cl_(.+)/ ){
    			    #print "1: $1\n";
    			    if ( exists $handles{$handle_to_id{$i}}{$1} ){
    				$nodes2{$freq}{$f}{$username_to_globalid{$i}}{$k} = $handles{$handle_to_id{$i}}{$1};
    			    }else{
    				$nodes2{$freq}{$f}{$username_to_globalid{$i}}{$k} = "-999";			
    			    }
    			}else{
    			    $nodes2{$freq}{$f}{$username_to_globalid{$i}}{$k} = "-999";			
    			}
    		    }
    		    #this has to be after above, so over-writes -999 for username...
    		    $nodes2{$freq}{$f}{$username_to_globalid{$i}}{'preferredUsername2'} = $i;
    		}
    	    }
    	}
    }
    #exit;
    
    # #(2) check 
    # #eventually might want to add seed nodes that aren't in twitter into here i.e. as isolates

    #print "payload: ".Dumper(\%payload)."\n";
    #exit;

    #need to modify graphml, adjacency, tsna as data structures have changed
    foreach my $freq ( @freq ){
	foreach my $f ( sort { $a cmp $ b } keys %{$nodes2{$freq}} ){
	    printPajek($seeds_full, $edgetype, $freq, $f, $nodes2{$freq}{$f}, $edges2{$freq}{$f}, \%payload, \%node_attr_client, $printPayload);
	}
    }

    print "finished networks()\n";

}


sub findAttributes{

    my ( $match, $node_attr_client_r, $freq_i ) = @_;
    my %node_attr_client = %{$node_attr_client_r};

    print "started findAttributes()\n";

    system("mkdir -p activity");       #create dir if necessary
    open my $F_out2, ">activity/potential_problem_data.txt" or die $!;
    print $F_out2 "The following seed accounts had changes in twitter usernames (sequence of usernames used during period).\n";
    print $F_out2 "Note: if the 'changed' usernames are in fact identical, this just means that the account holder changed capitalisation,\nand this can therefore be ignored.\n\n";

    my %handle_to_id = ();
    foreach my $i ( keys %handles ){
	$handle_to_id{$handles{$i}{$match}} = $i;
    }
    #print Dumper(\%handles)."\n";
    #exit;

    my %nodes2 = ();

    #The following loop over periods of data collection, constructs two hashes: 
    #  %nodes2 is the attributes of actors, at each time period e.g. monthly, weekly
    #  %edges2 is the edges created in each time period
    #  While %nodes and %edges have ulid id numbers as keys, remember that these are not constant across periods
    #  So %nodes2 and %edges2 use the usernames as keys.  Need to do this because it is possible that e.g. tweets for a given week
    #  come from two different periods of data and hence same actor has different ulid.  So need to use usernames.
    #  But note: the following (and all my code) assumes that usernames are not changing across periods i.e. if person changes usernames, we can't track this at present

    my %ulid_to_username = ();                 #this is id that comes with the graphml (NOT constant across periods)
    foreach my $p ( @periods ){
	print "Working on period: $p\n";
	my $fn = $ftag.$p;

	my %nodes = %{getHash2("hashes/$fn.nodes_hash")};	
	#print Dumper(\%nodes)."\n";
	#exit;
	#my %edges = %{getHash2("hashes/$fn.edges_hash", "gz")};
	#print Dumper(\%edges)."\n";
	#exit;
	#my %tweets = %{getHash2("hashes/$fn.tweets_hash", "gz")};
	#print Dumper(\%tweets)."\n";
	#exit;

	#my %ulid_to_username = ();                 #this is id that comes with the graphml (NOT constant across periods)
	my %username_to_ulid = ();
	my %usernames = ();                         #for checking if username changed...
	foreach my $i ( keys %nodes ){
            #By sorting on timestamp, will now have predictability re. constructed data (previously if username changed during period
	    #then might get original username or changed username and that affected things).
	    #Also this should be quicker, since username should be in first timestamp
	    foreach my $t ( sort { $a cmp $b } keys %{$nodes{$i}} ){
		next if ( $t eq 'network' );

		#new way - iterate over all timestamps so can check for username change....
		if ( exists $nodes{$i}{$t}{'preferredUsername2'} ){
		    if ( not exists $ulid_to_username{$p}{$i} ){        #first time: canonical username - use this
			#print Dumper($nodes{$i}{$t}) if ( $nodes{$i}{$t}{'preferredUsername2'} eq "nasca1995" );
			#print Dumper($nodes{$i}{$t}) if ( $i eq "n55998" );
			$ulid_to_username{$p}{$i} = $nodes{$i}{$t}{'preferredUsername2'};             #only seeds/target
			$username_to_ulid{$nodes{$i}{$t}{'preferredUsername2'}} = $i;
			#last;            #comment this out, so can check for username changes
			$usernames{$i} = [ $nodes{$i}{$t}{'preferredUsername2'} ];
		    }else{
			push @{$usernames{$i}}, $nodes{$i}{$t}{'preferredUsername2'};
		    }
		}
	    }
	}

	my %uniqid_to_ulid = ();
	my %ulid_to_uniqid = ();
	foreach my $i ( keys %handles ){
	    #need test for existence here??? see below...
	    my $ulid = $username_to_ulid{$handles{$i}{$match}};
	    $uniqid_to_ulid{$i} = $ulid;

	    $ulid_to_uniqid{$username_to_ulid{$handles{$i}{$match}}} = $i if ( exists $username_to_ulid{$handles{$i}{$match}} );    #some seeds may not be in Twitter collection

	    if ( $ulid ){
	    	if ( @{$usernames{$ulid}} > 1 ){
	    	    #die "problem: $ulid\n";
	    	    my $s = join(",", @{$usernames{$ulid}});
	    	    print $F_out2 "In period -$p-: $s\n";
	    	}
	    }
	}

	#Collect the node attribute data for each period
	#Data from nodes vector (attributes provided by Twitter) - stocks
	my $ii = 1;
	foreach my $i ( keys %nodes ){
	    my $u = $ulid_to_username{$p}{$i};
	    foreach my $t ( sort { $a cmp $b } keys %{$nodes{$i}} ){
		next if ( $t eq 'network' );                                  #eventually maybe remove this from %nodes
		next if ( !inRange($t, $minDate, 'node') );
		my %freq = %{findPeriods2($t, $freq_i)};
		#print "$t, $period_month, $period_week\n";

		#Data from Twitter
		foreach my $a ( @attr_twitter ){
		    if ( exists $nodes{$i}{$t}{$a} ){
			my $v = $nodes{$i}{$t}{$a};
			#The following recognises that we are collecting data on the actors at different times during the month/week
			#and so it gives the max. value of the attribute, over the period (month/week/all)
			foreach my $f ( keys %freq ){
			    if ( exists $nodes2{$f}{$freq{$f}}{$a} ){
				$nodes2{$f}{$freq{$f}}{$u}{$a} = $v if ( $v > $nodes2{$f}{$freq{$f}}{$u}{$a} );  #update for this period
			    }else{
				$nodes2{$f}{$freq{$f}}{$u}{$a} = $v;
			    }
			}
		    }
		}

		#Data from client (constant across months, but doesn't matter...)
		if ( exists $ulid_to_uniqid{$i} ){        #this is a 'seed' actor
		    foreach my $f ( keys %freq ){
			foreach my $a ( keys %node_attr_client ){
			    $nodes2{$f}{$freq{$f}}{$u}{"cl_".$a} = $handles{$ulid_to_uniqid{$i}}{$a};
			}
		    }
		}

		#Data for 'important' non-seed actors
		foreach my $k ( keys %important ){
		    if ( exists $important{$k}{$u} ){
			foreach my $f ( keys %freq ){
			    $nodes2{$f}{$freq{$f}}{$u}{"cl_".$k} = $important{$k}{$u};
			}
		    }
		}

	    }
	    print "node attributes: finished working on node -$ii-\n" if ( $ii%1000 == 0 );
	    $ii++;
	}
	my $n = keys %{$nodes2{'all'}{'all'}};
	print "number of actors: $n\n";
       	#print Dumper(\%nodes2)."\n";
	#exit;

    }
    close $F_out2;

    #Need to repair %nodes2 by filling in for missing data, but imputing value from most recent period where it was recorded.
    #I'm sure this was being done in the code for generating activity data, but need to do this with the network datasets too..
    my %recentValue = ();
    foreach my $f ( sort { $a cmp $b } keys %{$nodes2{$freq_i}} ){
    	print "f: $f\n";
    	foreach my $i ( keys %{$nodes2{$freq_i}{$f}} ){
	    foreach my $a ( @attr_twitter ){
		if ( not exists $nodes2{$freq_i}{$f}{$i}{$a} ){
		    print "missing: $f, $i, $a\n";
		    if ( exists $recentValue{$i}{$a} ){
			print "\tupdating with most recent value: $recentValue{$i}{$a}\n";
			$nodes2{$freq_i}{$f}{$i}{$a} = $recentValue{$i}{$a};
		    }else{
			print "\tthere is no recent value, so need to give missing value...\n";
			$nodes2{$freq_i}{$f}{$i}{$a} = "-999";
		    }
		}else{
		    $recentValue{$i}{$a} = $nodes2{$freq_i}{$f}{$i}{$a};
		}
	    }
    	}
    }
    #exit;

    #print Dumper(\%nodes2)."\n";
    #exit;
    print "finished findAttributes()\n";
    return (\%nodes2, \%ulid_to_username);

}


sub printPajek{

    my ( $seeds_full, $type, $freq, $f, $nodes_r, $edges_r, $payload_r, $node_attr_client_r, $printPayload ) = @_;
    print "printing pajek format: $type, $freq, $f...\n";
    my %nodes = %{$nodes_r};
    my %edges = %{$edges_r};
    my %payload = %{$payload_r};
    #print Dumper(\%edges)."\n";
    #print Dumper(\%nodes)."\n";
    #print Dumper(\%payload)."\n";
    #exit;

    my @node_attr_client = ();
    foreach my $i ( sort { $a cmp $b } keys %{$node_attr_client_r} ){
	push @node_attr_client, "cl_$i";
    }

    #-------------
    #Network
    #-------------
    system("mkdir -p pajek");
    open(my $out1, ">:gzip", "pajek/$tag.$type.$seeds_full.$freq.$f.net.gz") or die "Can't open file: $!";

    open(my $out3, ">:gzip", "pajek/$tag.$type.$seeds_full.$freq.$f.edge_attributes.csv.gz") or die "Can't open file: $!";
    print $out3 "edgeId,tweetId,edgeType,postedTime\n";

    my $nodes_n = keys %nodes;
    print $out1 "*Vertices $nodes_n\n";
    foreach my $i ( sort { $a <=> $b } keys %nodes ){
	print $out1 "$i \"$nodes{$i}{'preferredUsername2'}\"\n";
    }

    print $out1 "*Arcs\n";
    my $t = 1;
    my %inEdge = ();
    foreach my $i ( sort { $a <=> $b } keys %edges ){
	foreach my $j ( sort { $a <=> $b } keys %{$edges{$i}} ){
	    
	    #weighted version
	    #my $wt = keys %{$edges{$i}{$j}};
	    #print $out1 "$i $j $wt\n";

	    #multiple edge version
	    foreach my $k ( sort { $a <=> $b } keys %{$edges{$i}{$j}} ){
		print $out1 "$i $j 1\n";
		print $out3 "$t,\"$k\",\"$edges{$i}{$j}{$k}{'edgeType'}\",\"$edges{$i}{$j}{$k}{'postedTime'}\"\n";
		#print $out3 "$t,\"$k\",\"$edges{$i}{$j}{$k}{'postedTime'}\"\n";
		$inEdge{$k} = 1;
		$t++;
	    }
	}
    }
    close $out1;
    close $out3;

    #-------------
    #Tweet payload
    #-------------
    if ( $printPayload ){
	my @arr = ();
	foreach my $i ( sort { $a <=> $b } keys %payload ){
	    if ( exists $inEdge{$i} ){
		my %h = ();
		$h{'tweetId'} = $i;
		$h{'content'} = $payload{$i};
		push @arr, \%h;
	    }
	}
	my $json = JSON->new;
	open(my $out4, ">:utf8", "pajek/$tag.$type.$seeds_full.$freq.$f.payload.json") or die "Can't open file: $!";
	#print $out4 $json->encode(\@arr) . "\n";
	print $out4 $json->pretty->encode(\@arr) . "\n";
	#the following converts each hash (in the array) to a json string, and prints it
	#foreach my $i ( @arr ){
	#    print $out4 $json->encode($i) . "\n";
	#}
	close $out4;
	#note: with large datasets may need to do the following manually, as there were truncations with the following approach
	system("gzip pajek/$tag.$type.$seeds_full.$freq.$f.payload.json");   #do this manually, not sure about >:gzip+utf8 and this was truncating
    }

    #-------------
    #Vertex attributes
    #-------------
    open(my $out2, ">:gzip", "pajek/$tag.$type.$seeds_full.$freq.$f.vertex_attributes.csv.gz") or die "Can't open file: $!";           #1st entry - necessary
    my $s = "nodeId,preferredUsername2,";
    foreach my $j ( @attr_twitter ){
	$s .= "$j,";
    }
    foreach my $j ( @node_attr_client ){
	my $j_t = $j;
	$j_t =~ s/cl_//;             #remove "cl_"
	$s .= "$j_t,";
    }
    print $out2 substr($s,0,-1)."\n";	

    foreach my $i ( sort { $a <=> $b } keys %nodes ){
    	my $s = "$i,$nodes{$i}{'preferredUsername2'},";
    	foreach my $j ( @attr_twitter ){
    	#foreach my $j ( sort { $a cmp $b } keys %{$nodes{$i}} ){
    	    $s .= $nodes{$i}{$j}.",";
    	}
    	foreach my $j ( @node_attr_client ){
	    my $v = ( exists $nodes{$i}{$j} ) ? $nodes{$i}{$j} : "";
    	    $s .= "$v,";
    	}
    	print $out2 substr($s,0,-1)."\n";	
    }
    close $out2;
    
}


sub findPeriods2{
    my ( $t, $freq_i ) = @_;
    #print "t: $t\n";

    $t =~ /^(.+)-(.+)-(.+)T/;
    my $year = $1;
    my $month = $2;
    my $day = $3;
    my $week_n = Week_Number($year,$month,$day);
    $week_n = sprintf("%02d",$week_n);

    my %h = ();
    if ( $freq_i eq "monthly" ){
	$h{'monthly'} = $year."-".$month;
    }elsif ( $freq_i eq "weekly" ){
	$h{'weekly'} = $year."-".$week_n;
    }else{          #daily
	$h{'daily'} = $year."-".$month."-".$day;
    }
    $h{'all'} = "all";

    return \%h;

}


sub inRange{

    my ( $t, $minDate, $type ) = @_;

    $t =~ /^(.+)-(.+)-(.+)T/;
    my $year = $1;
    my $month = $2;
    my $day = $3;

    my $date = "$year-$month-$day";

    my $inRange = 1;
    if ( $date lt $minDate ){                         #can use lt/gt with yyyy-mm-dd
	print "\tOut of range ($type): $t\n";
	$inRange = 0;
    }
    return $inRange;

}


1;
