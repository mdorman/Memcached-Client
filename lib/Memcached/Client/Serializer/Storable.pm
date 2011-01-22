package Memcached::Client::Serializer::Storable;
#ABSTRACT: Implements Memcached Serializing using Storable

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use Storable qw{nfreeze thaw};
use base qw{Memcached::Client::Serializer};

use constant F_STORABLE => 1;

sub deserialize {
    my ($self, $data, $flags) = @_;

    return unless defined $data;

    $flags ||= 0;

    if ($flags & F_STORABLE) {
        DEBUG "Deserializing data";
        $data = thaw $data;
    }

    return $data;
}

sub serialize {
    my ($self, $command, $data) = @_;

    return unless defined $data;

    my $flags = 0;

    if (ref $data) {
        DEBUG "Serializing data";
        $data = nfreeze $data;
        $flags |= F_STORABLE;
    }

    return ($command, $data, $flags);
}

1;
