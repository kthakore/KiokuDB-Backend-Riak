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

ok( ! $d->lookup( $uuid )  );

#$backend->clear();

$backend->clear();
$d->store( { test => 'stuff', foo => { b_ar => 'stt' } }  );
$d->store( { test => 'stuff'}  );
$d->store( { test => 'stuff12'}  );
$d->store( { test => '123st12313'}  );

$d->update;

my @res = $d->search( { data_test => 'stuff', data_foo_b_ar => 'stt'  }, { inline => 'true' } )->all();

is_deeply( $res[0], { test => 'stuff', foo => { b_ar => 'stt' } }, 'Tests for search' );

$backend->clear;

done_testing;
