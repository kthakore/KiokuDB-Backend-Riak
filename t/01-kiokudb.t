use strict;
use warnings;
use Data::Dumper;
use Test::More;
use KiokuDB;
use KiokuDB::Backend::Riak;

my $backend = KiokuDB::Backend::Riak->new( 
            bucket_name => 'kbr_test' 
        ); 
my $d = KiokuDB->new(
        backend => $backend 
        );

my $s = $d->new_scope;
my $uuid = $d->store( { test => 'boo'}  );

ok($uuid, "Made an entry and sent it to riak");
my $data =  $d->lookup( $uuid);

is_deeply($data, {test => 'boo' } );

my $obj = $d->id_to_object( $uuid );

$obj->{test} = 'crap'; 

$d->update( $obj );

is_deeply( $d->lookup( $uuid ), {test => 'crap' } );

$d->delete( $uuid );

is_deeply( $d->lookup( $uuid ), '' );

#$backend->clear();


done_testing;
