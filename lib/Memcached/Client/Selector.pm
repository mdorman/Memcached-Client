package Memcached::Client::Selector;
BEGIN {
  $Memcached::Client::Selector::VERSION = '1.00';
}
# ABSTRACT: Abstract Base Class For Memcached::Client Selector

use strict;
use warnings;


sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}


sub set_servers {
    die "You must implement set_servers";
}


sub get_server {
    die "You must implement get_sock";
}

1;

__END__
=pod

=head1 NAME

Memcached::Client::Selector - Abstract Base Class For Memcached::Client Selector

=head1 VERSION

version 1.00

=head1 SYNOPSIS

  package NewHash;
  use strict;
  use base qw{Memcached::Client::Selector};

=head1 METHODS

=head2 new

C<new()> builds a new object.  It takes no parameters.

=head2 set_servers

C<set_servers()> will initialize the selector from the arrayref of
servers (or server => weight tuples) passed to it.

=head2 get_server

C<get_server()> will use the object's list of servers to extract the
proper server name from the list of connected servers, so the protocol
object can use it to make a request.

This routine can return undef, if called before set_servers has been
called, whether explicitly, or implicitly by handling a list of
servers to the constructor.

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

