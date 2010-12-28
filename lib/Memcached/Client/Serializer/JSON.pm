package Memcached::Client::Serializer::JSON;
BEGIN {
  $Memcached::Client::Serializer::JSON::VERSION = '1.06';
}
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

__END__
=pod

=head1 NAME

Memcached::Client::Serializer::JSON - Implements Memcached Serializing using JSON

=head1 VERSION

version 1.06

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

