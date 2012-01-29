use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'KiokuDB::Backend::Riak',
    AUTHOR              => q{Kartik Thakore <kthakore@cpan.org>},
    VERSION_FROM        => 'lib/KiokuDB/Backend/Riak.pm',
    ABSTRACT_FROM       => 'lib/KiokuDB/Backend/Riak.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
	'KiokuDB'    => 0,
        'Net::Riak'  => 0,
        'Try::Tiny'  => 0,
        'LWP::Simple' => 0,
        'Data::Stream::Bulk::Array' => 0,
        'Module::Pluggable' => 0
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'KiokuDB-Backend-Riak-*' },
);
