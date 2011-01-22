package Memcached::Client::Serializer::JSON;
#ABSTRACT: Implements Memcached Serializing using JSON

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use JSON qw{decode_json encode_json};
use base qw{Memcached::Client::Serializer};

use constant F_JSON => 4;

sub deserialize {
    my ($self, $data, $flags) = @_;

    return unless defined $data;

    $flags ||= 0;

    if ($flags & F_JSON) {
        DEBUG "Deserializing data";
        $data = decode_json $data;
    }

    return $data;
}

sub serialize {
    my ($self, $command, $data) = @_;

    return unless defined $data;

    my $flags = 0;

    if (ref $data) {
        DEBUG "Serializing data";
        $data = encode_json $data;
        $flags |= F_JSON;
    }

    return ($command, $data, $flags);
}

1;
