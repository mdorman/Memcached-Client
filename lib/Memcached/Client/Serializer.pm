package Memcached::Client::Serializer;
#ABSTRACT: Abstract Base Class For Memcached::Client Serializer

use strict;
use warnings;

=head1 SYNOPSIS

  package NewSerializer;
  use strict;
  use base qw{Memcached::Client::Serializer};

=method new

C<new()> builds a new object.  It takes no parameters.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    return $self;
}

=method deserialize()

C<deserialize()> will do its best to uncompress and/or deserialize the
data that has been returned.

=cut

sub deserialize {
    die "You must implement deserialize";
}

=method serialize()

C<serialize()> will serialize the data it is given (if it's a
reference), and if the data is large enough and the savings
significant enough (and the compression code is loadable), it will
compress it as well.

=cut

sub serialize {
    die "You must implement serialize";
}

1;
