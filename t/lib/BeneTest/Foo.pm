package BeneTest::Foo;
use Moose;

has 'name' => ( is => 'rw', isa => 'Str' );
has 'bar' => ( is=> 'rw', isa => 'Bar', default => sub { Bar->new( stuff => 'gen' ) } );

1;

