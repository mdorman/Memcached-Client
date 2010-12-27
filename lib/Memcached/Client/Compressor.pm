package Memcached::Client::Compressor;
#ABSTRACT: Abstract Base Class For Memcached::Client Compressor

use strict;
use warnings;

=head1 SYNOPSIS

  package NewCompresor;
  use strict;
  use base qw{Memcached::Client::Compressor};

=method new

C<new()> builds a new object.  It takes no parameters.

=cut

sub new {
    my $class = shift;
    my $self = bless {compress_threshold => 0}, $class;
    return $self;
}

=method compress_threshold()

Retrieve or change the compress_threshold value.

=cut

sub compress_threshold {
    my ($self, $new) = @_;
    my $ret = $self->{compress_threshold};
    $self->{compress_threshold} = $new if ($new);
    return $ret;
}

=method decompress()

C<decompress()> will do its best to uncompress and/or deserialize the
data that has been returned.

=cut

sub decompress {
    die "You must implement decompress";
}

=method compress()

C<compress()> will (if the compression code is loadable) compress the
data it is given, and if the data is large enough and the savings
significant enough, it will compress it as well.

=cut

sub compress {
    die "You must implement compress";
}

1;
