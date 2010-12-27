#!/usr/bin/perl

use Memcached::Client::Serializer::JSON;
use JSON::XS qw{encode_json};
use Test::More tests => 11;

my $serializer;

isa_ok ($serializer = Memcached::Client::Serializer::JSON->new,
        'Memcached::Client::Serializer::JSON',
        'Create a new instance of the ::JSON class');

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

my $longjson = encode_json $longref;

is_deeply ($serializer->serialize ($longref),
           {data => $longjson, flags => 4},
           '->serialize a very long repetitive string inside a ref');

is_deeply ($serializer->deserialize ({data => $longjson, flags => 4}),
           {data => $longref, flags => 4},
           '->deserialize a very long repetitive string inside a ref, compare');
