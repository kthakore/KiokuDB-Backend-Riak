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
        'Net::Riak'  => 0,
        'WebService::Solr::Query' => 0
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'KiokuDB-Backend-Riak-*' },
);
