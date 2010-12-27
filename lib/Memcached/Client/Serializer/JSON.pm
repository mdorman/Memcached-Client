package Memcached::Client::Serializer::JSON;
#ABSTRACT: Implements Memcached Serializing using JSON

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use JSON qw{decode_json encode_json};
use base qw{Memcached::Client::Serializer};

use constant F_JSON => 4;

sub deserialize {
    my ($self, $tuple) = @_;

    return unless defined $tuple->{data};

    $tuple->{flags} ||= 0;

    if ($tuple->{flags} & F_JSON) {
        DEBUG "Deserializing data";
        $tuple->{data} = decode_json $tuple->{data};
    }

    return $tuple;
}

sub serialize {
    my ($self, $data) = @_;

    return unless defined $data;

    my $tuple = {flags => 0};

    if (ref $data) {
        DEBUG "Serializing data";
        $tuple->{data} = encode_json $data;
        $tuple->{flags} |= F_JSON;
    } else {
        $tuple->{data} = $data;
    }

    return $tuple;
}

1;
