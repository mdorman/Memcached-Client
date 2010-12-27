#!/usr/bin/perl

use Memcached::Client::Serializer::Storable;
use Storable qw{nfreeze};
use Test::More tests => 11;

my $serializer;

isa_ok ($serializer = Memcached::Client::Serializer::Storable->new,
        'Memcached::Client::Serializer::Storable',
        'Create a new instance of the ::Storable class');

is ($serializer->serialize,
    undef,
    '->serialize should return undef since we gave it nothing to serialize');

is ($serializer->deserialize,
    undef,
    '->deserialize should return undef since we gave it nothing to deserialize');

is_deeply ($serializer->serialize ('foo'),
           {data => 'foo', flags => 0},
           '->serialize should return the simple tuple since it is so short');

is_deeply ($serializer->deserialize ({data => 'foo', flags => 0}),
           {data => 'foo', flags => 0},
           '->deserialize should return the same structure since it was not serialized');

is_deeply ($serializer->serialize ('17times3939'),
           {data => '17times3939', flags => 0},
           '->serialize should return the simple tuple since it is so short');

is_deeply ($serializer->deserialize ({data => '17times3939', flags => 0}),
           {data => '17times3939', flags => 0},
           '->deserialize should return the same tuple since it was not serialized');

my $longstring = 'a' x 20000;

is_deeply ($serializer->serialize ($longstring),
           {data => $longstring, flags => 0},
           '->serialize a very long repetitive string');

is_deeply ($serializer->deserialize ({data => $longstring, flags => 0}),
           {data => $longstring, flags => 0},
           '->deserialize our very long repetitive string, compare');

my $longref = {longstring => $longstring};

my $longfreeze = nfreeze $longref;

is_deeply ($serializer->serialize ($longref),
           {data => $longfreeze, flags => 1},
           '->serialize a very long repetitive string inside a ref');

is_deeply ($serializer->deserialize ({data => $longfreeze, flags => 1}),
           {data => $longref, flags => 1},
           '->deserialize a very long repetitive string inside a ref, compare');
