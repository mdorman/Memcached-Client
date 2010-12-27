package Memcached::Client::Serializer::Storable;
#ABSTRACT: Implements Memcached Serializing using Storable

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use Storable qw{nfreeze thaw};
use base qw{Memcached::Client::Serializer};

use constant F_STORABLE => 1;

sub deserialize {
    my ($self, $tuple) = @_;

    return unless defined $tuple->{data};

    $tuple->{flags} ||= 0;

    if ($tuple->{flags} & F_STORABLE) {
        DEBUG "Deserializing data";
        $tuple->{data} = thaw $tuple->{data};
    }

    return $tuple
}

sub serialize {
    my ($self, $data) = @_;

    return unless defined $data;

    my $tuple = {flags => 0};

    if (ref $data) {
        DEBUG "Serializing data";
        $tuple->{data} = nfreeze $data;
        $tuple->{flags} |= F_STORABLE;
    } else {
        $tuple->{data} = $data;
    }

    return $tuple;
}

1;
