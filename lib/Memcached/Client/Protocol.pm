package Memcached::Client::Protocol;
# ABSTRACT: Base Class For Memcached::Client Protocol implementations

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG LOG};

=head1 SYNOPSIS

  package Memcached::Client::Protocol::NewProtocol;
  use strict;
  use base qw{Memcached::Client::Protocol};

=method new

C<new()> creates the protocol object.

=cut

sub new {
    my $class = shift;
    my $self = bless {@_}, $class;
    return $self;
}

=method C<decode>

=cut

sub decode {
    my ($self, $data, $flags) = @_;
    return $self->{serializer}->deserialize ($self->{compressor}->decompress ($data, $flags));
}

=method C<encode>

=cut

sub encode {
    my ($self, $command, $value) = @_;
    if ($command ne 'append' && $command ne 'prepend') {
        $self->log ("Encoding request data") if DEBUG;
        return $self->{compressor}->compress ($self->{serializer}->serialize ($value));
    } else {
        $self->log ("Nothing to do") if DEBUG;
        return $value, 0;
    }
}


=method C<log>

Log the specified message with an appropriate prefix derived from the
class name.

=cut

sub log {
    my ($self, $format, @args) = @_;
    my $prefix = ref $self || $self;
    $prefix =~ s,Memcached::Client::Protocol::,Protocol/,;
    LOG ("$prefix> " . $format, @args);
}

=method prepare_handle

This routine is handed the raw file handle before any connection is
done, for any massaging the procotol may need to do to it (this is
typically just the binary protocol setting binmode to true).

=cut

sub prepare_handle {
    return sub {};
}

=method rlog

Knows how to extract information from connections and requests.

=cut

sub rlog {
    my ($self, $connection, $request, $message) = @_;
    my $prefix = ref $self || $self;
    $prefix =~ s,Memcached::Client::Protocol::,Protocol/,;
    LOG ("$prefix/%s> %s = %s", $connection->{server}, join (" ", $request->{command}, $request->{key}), $message);
}


1;
