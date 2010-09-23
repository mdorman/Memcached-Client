package Memcached::Client::Protocol;
# ABSTRACT: Base Class For Memcached::Client Protocol implementations

use strict;
use warnings;

=head1 SYNOPSIS

  package Memcached::Client::Protocol::NewProtocol;
  use strict;
  use base qw{Memcached::Client::Protocol};

=method new

C<new()> creates the protocol object.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}

=method prepare_handle

This routine is handed the raw file handle before any connection is
done, for any massaging the procotol may need to do to it (this is
typically just the binary protocol setting binmode to true).

=cut

sub prepare_handle {
    return sub {};
}

1;
