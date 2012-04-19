package KiokuDB::Backend::Riak;

BEGIN {
    $KiokuDB::Backend::Riak::VERSION = '0.01';
}

use Moose;
use Data::Dumper;
use Try::Tiny;
use lib 'lib';
use Net::Riak;
use JSON ();
use LWP::Simple ();

use KiokuDB::Backend::Riak::Query;
use KiokuDB::Backend::Serialize::JSPON::Collapser;
use KiokuDB::Backend::Serialize::JSPON::Expander;

use Data::Stream::Bulk::Array ();

use namespace::clean -except => 'meta';


with qw(
  KiokuDB::Backend
  KiokuDB::Backend::Serialize::JSON
  KiokuDB::Backend::Role::Clear
  KiokuDB::Backend::Role::Scan
  KiokuDB::Backend::Role::Query::Simple
  KiokuDB::Backend::Role::Query
  KiokuDB::Backend::Role::BinarySafe
);

has [qw/host port bucket_name/] => (
    is  => 'ro',
    isa => 'Str'
);


has 'schema' => (
    is => 'rw',
    isa => 'Str'
);

has '_url' => (
    is  => 'rw',
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
    my $host = $self->host || $ENV{KRB_HOST} || 'localhost';
    my $port = $self->port || $ENV{KRB_PORT} || 8098;
    my $buck_name  = $self->bucket_name;
    my $options = $self->options;
    my $uri = 'http://' . $host . ':' . $port;
    $self->_url($uri);
    my $client = Net::Riak->new( host => $uri, %$options );
     
    my $bucket = $client->bucket($buck_name);

    # Hook in kv_search into the precommit so that we can start indexing. Custom indexes will have to be set elsewhere

     #bucket doesn't exist in riak unless it has object in it. Can't enable search on empty bucket. So touching the bucket
    my $touch = $bucket->new_object( 'touch',  { 'KiokuDB-Backend-Riak' => 'Initialized' } );

    my $props = $bucket->get_property('precommit'); 

    unless( ref $props eq 'ARRAY' && $#{$props} > 0 )
    {
      $props = [{"mod"=>"riak_search_kv_hook","fun"=>"precommit"}];
      $bucket->set_property( 'precommit', $props );


    }
    

    return $bucket;

}

sub load_schema {
	my ($self, %args) = @_;
	require Module::Pluggable;
	my $shorten = delete $args{shorten};
	my $search_path = delete $args{search_path};
	Module::Pluggable->import ( search_path => $search_path );
	for my $module ($self->plugins ) {
		eval "require $module";
		croak $@ if $@; 
		if ($shorten && $module =~ m/$search_path\:\:(.*?)$/ ) {
			my $short_name = $1;

			no strict 'refs';
			*{ $short_name . "::" } = \*{ $module . "::" };
			$short_name->meta->{kbr_schema_config} =
			$module->meta->{kbr_schema_config};

		}
	}

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
    my ( $self, @entries ) = @_;

    my $bucket = $self->bucket;
    my $c = KiokuDB::Backend::Serialize::JSPON::Collapser->new(
        id_field => "id",
    );

    for my $entry (@entries) {
        my $collapsed = $c->collapse_jspon($entry);;

        my $id        = delete $collapsed->{id};

        my $obj = $bucket->get($id);
        $obj->data($collapsed);
        $obj->store();

    }
}

sub get {
    my ( $self, @ids ) = @_;
    return map { $self->get_entry($_); } @ids;
}

sub get_entry {
    my ( $self, $id ) = @_;
    my $url = $self->_url;
       $url .= '/riak/'.$self->bucket_name.'/'.$id;
    my $obj = LWP::Simple::get($url);

    return undef unless $obj;
    my $d = $self->deserialize($obj);
       $d->{id} = $id;
    my $c = KiokuDB::Backend::Serialize::JSPON::Expander->new( 
        id => 'id'
    );
    return $c->expand_jspon( $d );
    
}

sub get_entry_raw {
    my ( $self, $id ) = @_;
    my $url = $self->_url;
       $url .= '/riak/'.$self->bucket_name.'/'.$id;
    my $obj = LWP::Simple::get($url);
    return '{ }' unless $obj;
    return $obj;   
}

sub delete {
    my ( $self, $id ) = @_;

    $self->bucket->delete_object($id);
}

sub simple_search {
    my ( $self, $proto, $args  ) = @_;
    return $self->search($proto, $args);
}

sub search {
    my ( $self, $proto, $args ) = @_;

    my $url = $self->_url;
    $url .= '/solr/' . $self->bucket_name . '/select/';
    
    my $q = KiokuDB::Backend::Riak::Query->new($proto)->stringify;

    $url .= "?q=$q&wt=json";

    if( $args && ref $args eq 'HASH' )
    {
        foreach my $k ( keys %${args} )
        {
            my $v = $args->{$k};
            $url .= '&'.$k.'='.$v;
        }
    }

    if( $ENV{KBR_DEBUG} ) { warn 'DEBUG KiokuDB::Backend::Riak URL Search'. $url }

    my $solr = LWP::Simple::get($url);

    my $res = JSON::decode_json($solr);

    my @objs = map { $self->deserialize( JSON::encode_json($_) ) } @{$res->{response}->{docs}};

    return Data::Stream::Bulk::Array->new( array => \@objs );
}


sub search_raw {
    my ( $self, $proto, $args ) = @_;

    my $url = $self->_url;
    $url .= '/solr/' . $self->bucket_name . '/select/';
    
    my $q = KiokuDB::Backend::Riak::Query->new($proto)->stringify;

    $url .= "?q=$q&wt=json";

    if( $args && ref $args eq 'HASH' )
    {
        foreach my $k ( keys %${args} )
        {
            my $v = $args->{$k};
            $url .= '&'.$k.'='.$v;
        }
    }

    if( $ENV{KBR_DEBUG} ) { warn 'DEBUG KiokuDB::Backend::Riak URL Search'. $url }

    my $solr = LWP::Simple::get($url);

    return $solr


}


sub exists {
    my ( $self, @ids ) = @_;
    my $bucket = $self->bucket;
    return map { $bucket->get($_)->exists } @ids;
}

sub serialize {
    my $self = shift;
    return $self->collapse_json(@_);
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

Check if obj exists with given id 

=head2 insert 

Inserts OBJ

=head2 clear 

Clears all objects in bucket

=head2 all_entries 

Gets all entries

=head2 get 

Get array of entries

=head2 get_entry  

get specific entry

=head2 get_entry_raw

get specific entry raw

=head2 delete 

Deletes object

=head2 serialize 

Serializes to JSON

=head2 deserialize

Deserializes from JSON

=head2 search 

Searches using Solr interface to riak search. To search must prefix with data_ as KiokuDB stores data in there. Also nested objects must be seperated by '_'

Note to enable search be sure to have bin/search-cmd. 

Edit the etc/app.config

            {riak_search, [                                                                                                                                                                             
                %% To enable Search functionality set this 'true'.                                                                                                                           
                                {enabled, true}                                                                                                                                                              
                                               ]}

Then ensure index on your bucket by doing

bin/search-cmd install BUCKET 

=head2 search_raw

search results in raw json

=head2 simple_search

Searches using Solr interface to riak search. To search must prefix with data_ as KiokuDB stores data in there. Also nested objects must be seperated by '_'

=head2 BUILD

Nothing to do about Nothing

=head2 load_schema 

Loads document schemas for quick use. Credited to authors of Mongoose library.

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
