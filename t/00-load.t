#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'KiokuDB::Backend::Riak' ) || print "Bail out!
";
}

diag( "Testing KiokuDB::Backend::Riak $KiokuDB::Backend::Riak::VERSION, Perl $], $^X" );
