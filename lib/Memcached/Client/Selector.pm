package Memcached::Client::Selector;
# ABSTRACT: Abstract Base Class For Memcached::Client Selector

use strict;
use warnings;
use Memcached::Client::Log qw{LOG};

=head1 SYNOPSIS

  package NewHash;
  use strict;
  use base qw{Memcached::Client::Selector};

=method new

C<new()> builds a new object.  It takes no parameters.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}

=method C<log>

Log the specified message with an appropriate prefix derived from the
class name.

=cut

sub log {
    my ($self, $format, @args) = @_;
    my $prefix = ref $self || $self;
    $prefix =~ s,Memcached::Client::Selector::,Selector/,;
    LOG ("$prefix> " . $format, @args);
}

=method set_servers

C<set_servers()> will initialize the selector from the arrayref of
servers (or server => weight tuples) passed to it.

=cut

sub set_servers {
    die "You must implement set_servers";
}

=method get_server

C<get_server()> will use the object's list of servers to extract the
proper server name from the list of connected servers, so the protocol
object can use it to make a request.

This routine can return undef, if called before set_servers has been
called, whether explicitly, or implicitly by handling a list of
servers to the constructor.

=cut

sub get_server {
    die "You must implement get_sock";
}

1;
