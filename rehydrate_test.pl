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

my $funcDir = ".";

require "$funcDir/rehydrate_functions.pl";

my $start_run = time();

our $tag = "test";
our $ftag = "twitter_multinet_$tag"."_";

#following used to exclude activites earlier than collection period (e.g. tweets that have been retweeted)
#also, actors with no profile data (because haven't created activity matching target query) given this 'collDate' for profile entry
#this ensures the actors are included in the network
our $minDate = "2015-09-01";               

our @periods = ('period1','period2');  #testing...

#----------------------------
#1. Construct the hashes
#   Only need to run once.
#----------------------------
#&constructHashes();
#exit;

#----------------------------
#2. Construct the networks
#----------------------------
&constructNetworks('multi');               #all edge types
#&constructNetworks('mention');
#&constructNetworks('reply');
#&constructNetworks('retweet');
#&constructNetworks('self-loop');


sub constructHashes{

    foreach my $p ( @periods ){
	my $fn = $ftag.$p;
	&constructHash_lineParse_new($fn);
    }

}


sub constructNetworks{

    my $edgetype = shift;

    our @sample = ();              #use if no sample
    #our @sample = ('acoss', 'ausrepublic');

    our %handles = ();             #use if no 'seed' accounts (e.g. with user-supplied attributes)

    #frequency of the networks and activity data
    #my $freq_i = 'monthly';
    #my $freq_i = 'weekly';
    my $freq_i = 'daily';

    my $match = "";              #this is the name of column in the user-supplied attributes file that contains twitter handle (blank if none)  
    my %node_attr_client = ();   #user-supplied attributes to feature in the networks
    my ( $nodes2_r, $ulid_to_username_r ) = findAttributes($match, \%node_attr_client, $freq_i);
    my %nodes2 = %{$nodes2_r};
    my %ulid_to_username = %{$ulid_to_username_r};
    #print "nodes2: ".Dumper(\%nodes2)."\n";
    #exit;

    &networks($match, "all", $freq_i, $edgetype, \%node_attr_client, \%nodes2, \%ulid_to_username, 1);

}

