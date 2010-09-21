#!/usr/bin/perl

use Memcached::Client::Serializer::Storable;
use Test::More tests => 15;

my $serializer;

isa_ok ($serializer = Memcached::Client::Serializer::Storable->new,
     'Memcached::Client::Serializer::Storable',
     'Create a new instance of the ::Storable class');

is ($serializer->compress_threshold (10000),
    undef,
    'Check default compress_threshold');

is ($serializer->compress_threshold,
    10000,
    'Check recently set compress_threshold');

is ($serializer->serialize,
    undef,
    '->serialize should return undef since we gave it nothing to serialize');

is ($serializer->deserialize,
    undef,
    '->deserialize should return undef since we gave it nothing to deserialize');

is_deeply ([$serializer->serialize ('foo')],
    ['foo', 0, 3],
    '->serialize should return the simple string since it is so short');

is ($serializer->deserialize ('foo', 0),
    'foo',
    '->deserialize should return the simple string since it was not serialized');

is_deeply ([$serializer->serialize ('17times3939')],
    ['17times3939', 0, 11],
    '->serialize should return the simple string since it is so short');

is ($serializer->deserialize ('17times3939', 0),
    '17times3939',
    '->deserialize should return the simple string since it was not serialized');

my $longstring = 'a' x 20000;

my ($data, $flags);

ok (($data, $flags) = $serializer->serialize ($longstring), '->serialize a very long repetitive string');

is ($flags, 2, 'Make sure long repetitive string was compressed');

is ($serializer->deserialize ($data, $flags), $longstring, '->deserialize our very long repetitive string, compare');

my $longref = {longstring => $longstring};

ok (($data, $flags) = $serializer->serialize ($longref), '->serialize a very long repetitive string inside a ref');

is ($flags, 3, 'Make sure long repetitive string was compressed');

is_deeply ($serializer->deserialize ($data, $flags), $longref, '->deserialize our very long repetitive string inside a ref, compare');
