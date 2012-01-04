use strict;
use warnings;
use Test::More;

use KiokuDB;
use KiokuDB::Backend::Riak;

my $d = KiokuDB->new(
        backend => KiokuDB::Backend::Riak->new( 
            bucket_name => 'kbr_test' 
        ) 
        );

my $s = $d->new_scope;
my $uuid = $d->store( { test => 'boo'}  );



done_testing;
