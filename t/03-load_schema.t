package main;
use strict;
use warnings;
use Data::Dumper;
use Test::More;
use KiokuDB;
use KiokuDB::Backend::Riak;

use lib 't/lib';

my $backend = KiokuDB::Backend::Riak->new( 
            bucket_name => 'kbr_test'.rand() 
        ); 
   $backend->load_schema( shorten => 1, search_path => 'BeneTest' );
my $d = KiokuDB->new(
        backend => $backend 
        );

my $s = $d->new_scope;
$backend->clear;

my $foo = Foo->new( name => 'Stuff' );

my $uuid = $d->store( $foo );

$d->update();

my $back = $d->lookup( $uuid );

isa_ok( $back, 'Foo' );

isa_ok( $back->bar, 'Bar' );

my $buuid = $d->object_to_id( $back->bar );

my @res = $d->search( { 'data_bar_$ref' => $buuid.'*' } )->all;

is_deeply( $res[0], $foo, 'Test for class searching' );

$backend->clear;

done_testing; 

