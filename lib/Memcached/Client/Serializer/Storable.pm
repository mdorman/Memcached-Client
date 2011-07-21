package Memcached::Client::Serializer::Storable;
BEGIN {
  $Memcached::Client::Serializer::Storable::VERSION = '1.07';
}
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

__END__
=pod

=head1 NAME

Memcached::Client::Serializer::Storable - Implements Memcached Serializing using Storable

=head1 VERSION

version 1.07

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

