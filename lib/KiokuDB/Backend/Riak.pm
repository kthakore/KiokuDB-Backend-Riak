package KiokuDB::Backend::Riak;

BEGIN {
    $KiokuDB::Backend::Riak::VERSION = '0.03';
}

use Moose;
use Data::Dumper;
use Try::Tiny;
use Net::Riak;
use Data::Stream::Bulk::Callback ();

use namespace::clean -except => 'meta';

with qw(
  KiokuDB::Backend
  KiokuDB::Backend::Serialize::JSPON
  KiokuDB::Backend::Role::Clear
  KiokuDB::Backend::Role::Scan
  KiokuDB::Backend::Role::Query::Simple
  KiokuDB::Backend::Role::Query
);

has [qw/host port bucket_name/] => (
    is  => 'ro',
    isa => 'Str'
);

has options => (
    is  => 'ro',
    isa => 'HashRef'
);

has bucket => (
    isa     => 'Net::Riak::Bucket',
    is      => 'ro',
    lazy    => 1,
    builder => '_build_bucket'

);

has '+id_field'         => ( default => 'id' );
has '+class_field'      => ( default => 'class' );
has '+class_meta_field' => ( default => 'class_meta' );

sub _build_bucket {
    my ($self) = @_;
    my $host = $self->host || 'localhost';
    my $port = $self->port || 8091;
    my $bucket  = $self->bucket_name;
    my $options = $self->options;

    my $client =
      Net::Riak->new( host => 'http://' . $host . ':' . $port, %$options );

    return $client->bucket($bucket);

}

sub BUILD {
    my ($self) = shift;
    $self->bucket;
}

sub clear {
    my $self = shift;
    my $keys = $self->bucket->get_keys();
    foreach ( @{$keys} ) {
        $self->bucket->delete_object($_);
    }
}

sub all_entries {
    my $self = shift;

    return;
}

sub insert { 
    my ($self, @entries) = @_;

    my $bucket = $self->bucket;

    for my $entry (@entries) {
        my $collapsed = $self->serialize( $entry );
        my $id = $collapsed->{id};

        my $obj = $bucket->get( $id );
        $obj->data( $collapsed );
        $obj->store(); 

    }
}

sub get {
     my ($self, @ids) = @_;
     return map {
        $self->get_entry($_);    
     } @ids; 
}

sub get_entry { 
    my ($self, $id) = @_;
    my $obj = $self->bucket->get($id);    
    return undef unless $obj->exists;
    return $self->deserialize($obj);
}

sub delete {
    my ($self, $id) = @_;
    
    $self->bucket->delete_object( $id );    
}

sub simple_search { }

sub search { }

sub exists { }

sub serialize { 
    my $self = shift;
       return $self->collapse_jspon(@_);
}

sub deserialize { 
    my ($self,$doc, @args) = @_;;

    $self->expand_jspon($doc, @args);
    
}

=head1 NAME

KiokuDB::Backend::Riak - The great new KiokuDB::Backend::Riak!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use KiokuDB::Backend::Riak;

    my $foo = KiokuDB::Backend::Riak->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=head2 exists

=head2 insert 

=head2 clear 

=head2 all_entries 

=head2 get 

=head2 get_entry  

=head2 delete 

=head2 serialize 

=head2 deserialize

=head2 search 

=head2 simple_search

=head2 BUILD

=head1 AUTHOR

Kartik Thakore, C<< <kthakore at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-kiokudb-backend-riak at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=KiokuDB-Backend-Riak>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc KiokuDB::Backend::Riak


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=KiokuDB-Backend-Riak>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/KiokuDB-Backend-Riak>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/KiokuDB-Backend-Riak>

=item * Search CPAN

L<http://search.cpan.org/dist/KiokuDB-Backend-Riak/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Kartik Thakore.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;    # End of KiokuDB::Backend::Riak
