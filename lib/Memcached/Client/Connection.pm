package Memcached::Client::Connection;
BEGIN {
  $Memcached::Client::Connection::VERSION = '0.99';
}
# ABSTRACT: Class to manage Memcached::Client server connections

use strict;
use warnings;
use AnyEvent::Handle qw{};
use Memcached::Client::Log qw{DEBUG INFO};


sub new {
    my ($class, $server, $preparation) = @_;
    my $self = bless {prepare => $preparation, queue => [], server => $server}, $class;
    return $self;
}


sub connect {
    my ($self) = @_;

    my ($host, $port) = split (/:/, $self->{server});
    $port ||= 11211;

    # DEBUG "C [%s]: connecting to [%s:%s]", $self->{server}, $host, $port;

    $self->{handle} = AnyEvent::Handle->new (connect => [$host, $port],
                                             keepalive => 1,
                                             on_connect => sub {
                                                 my ($handle, $host, $port) = @_;
                                                 # DEBUG "C [%s]: connected", $self->{server};
                                                 $self->dequeue;
                                             },
                                             on_error => sub {
                                                 my ($handle, $fatal, $message) = @_;
                                                 # DEBUG "C [%s]: %s error %s", $self->{server}, ($fatal ? "fatal" : "non-fatal"), $message;
                                                 $self->fail;
                                                 $handle->destroy if ($handle);
                                                 undef $self->{handle};
                                             },
                                             on_prepare => sub {
                                                 my ($handle) = @_;
                                                 # DEBUG "C [%s]: preparing handle", $self->{server};
                                                 $self->{prepare}->($handle);
                                                 return $self->{connect_timeout} || 5;
                                             },
                                             peername => $self->{server});
}


sub enqueue {
    my ($self, $request, $failback) = @_;
    $self->connect unless ($self->{handle});
    # DEBUG "C [%s]: queuing request", $self->{server};
    push @{$self->{queue}}, {request => $request, failback => $failback};
    $self->dequeue;
    return 1;
}


sub dequeue {
    my ($self) = @_;
    return if ($self->{executing});
    # DEBUG "C [%s]: checking for job", $self->{server};
    return unless ($self->{executing} = shift @{$self->{queue}});
    # DEBUG "C [%s]: executing", $self->{server};
    $self->{executing}->{request}->($self->{handle},
                                    sub {
                                        # DEBUG "C [%s]: done with request", $self->{server};
                                        delete $self->{executing};
                                        $self->dequeue;
                                    },
                                    $self->{server});
}


sub fail {
    my ($self) = @_;
    # DEBUG "C [%s]: failing requests", $self->{server};
    if (my $current = delete $self->{executing}) {
        # DEBUG "C [%s]: failing current", $self->{server};
        $current->{failback}->();
    }
    if (my $current = delete $self->{queue}) {
        for my $request (@{$current}) {
            # DEBUG "C [%s]: invoking failback", $self->{server};
            $request->{failback}->();
        }
    }
    $self->{queue} = [];
}

1;

__END__
=pod

=head1 NAME

Memcached::Client::Connection - Class to manage Memcached::Client server connections

=head1 VERSION

version 0.99

=head1 SYNOPSIS

  use Memcached::Client::Connection;
  my $connection = Memcached::Client::Connection->new ("server:port");
  $connection->enqueue ($request->($handle, $callback), $failback);

=head1 METHODS

=head2 new

C<new()> builds a new connection object.

Its only parameter is the server specification, in the form of
"hostname:port".  The object is constructed and returns immediately.

=head2 connect

C<connect()> initiates a connection to the specified server.

If it succeeds, it will start dequeuing requests for the server to
satisfy.

If it fails, it will respond to all outstanding requests by invoking
their failback routine.

=head2 enqueue

C<enqueue()> adds the request specified (request, failback) pair to
the queue of requests to be processed.

=head2 dequeue

C<dequeue()> manages the process of pulling requests off the queue and
executing them as possible.  Each request is handed the handle for the
server connection as well as a callback that will mark it as done and
do the next request.

If the request code fails to invoke this callback, processing will
halt.

=head2 fail

C<fail()> is called when there is an error on the handle, and it
invokes the failbacks of all queued requests.

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

