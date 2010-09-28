package Memcached::Client::Protocol;
BEGIN {
  $Memcached::Client::Protocol::VERSION = '1.01';
}
# ABSTRACT: Base Class For Memcached::Client Protocol implementations

use strict;
use warnings;


sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}


sub prepare_handle {
    return sub {};
}

1;

__END__
=pod

=head1 NAME

Memcached::Client::Protocol - Base Class For Memcached::Client Protocol implementations

=head1 VERSION

version 1.01

=head1 SYNOPSIS

  package Memcached::Client::Protocol::NewProtocol;
  use strict;
  use base qw{Memcached::Client::Protocol};

=head1 METHODS

=head2 new

C<new()> creates the protocol object.

=head2 prepare_handle

This routine is handed the raw file handle before any connection is
done, for any massaging the procotol may need to do to it (this is
typically just the binary protocol setting binmode to true).

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

