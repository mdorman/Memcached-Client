package Memcached::Client::Connection;
BEGIN {
  $Memcached::Client::Connection::VERSION = '1.07';
}
# ABSTRACT: Class to manage Memcached::Client server connections

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Memcached::Client::Log qw{DEBUG INFO};
use Scalar::Util qw{weaken};


sub new {
    my ($class, $server, $preparation) = @_;
    my $self = bless {attempts => 0, last => 0, prepare => $preparation, requests => 0, queue => [], server => $server}, $class;
    return $self;
}


sub connect {
    my ($self, $callback) = @_;
    my $weak = $self; weaken ($weak);

    my ($host, $port) = split (/:/, $self->{server});
    $port ||= 11211;

    DEBUG "C [%s]: connecting to [%s:%s]", $self->{server}, $host, $port;

    $callback->() if $self->{handle};
    $self->{handle} ||= AnyEvent::Handle->new (connect => [$host, $port],
                                               keepalive => 1,
                                               on_connect => sub {
                                                   my ($handle, $host, $port) = @_;
                                                   DEBUG "C [%s]: connected", $weak->{server};
                                                   $weak->{attempts} = 0;
                                                   $weak->{last} = 0;
                                                   $weak->{requests} = 0;
                                                   $callback->() if ($callback);
                                                   $weak->dequeue;
                                               },
                                               on_error => sub {
                                                   my ($handle, $fatal, $message) = @_;
                                                   my $last = $weak->{last} ? AE::time - $weak->{last} : 0;
                                                   my $pending = scalar @{$weak->{queue}};
                                                   # Need this here in case connection fails
                                                   if ($message eq "Broken pipe") {
                                                       DEBUG "Requeueing broken pipe for %s", $weak->{server};
                                                       delete $weak->{handle};
                                                       $weak->connect;
                                                   } elsif ($message eq "Connection timed out" and ++$weak->{attempts} < 5) {
                                                       DEBUG "Requeueing connection timeout for %s", $weak->{server};
                                                       delete $weak->{handle};
                                                       $weak->connect ($callback);
                                                   } else {
                                                       INFO "C [%s]: %s, %d attempts, %d completed, %d pending, %f last", $weak->{server}, $message, $weak->{attempts}, $weak->{requests}, $pending, $last;
                                                       $callback->() if ($callback);
                                                       delete $weak->{handle};
                                                       $weak->fail;
                                                   }
                                               },
                                               on_prepare => sub {
                                                   my ($handle) = @_;
                                                   DEBUG "C [%s]: preparing handle", $weak->{server};
                                                   $weak->{prepare}->($handle) if ($weak->{prepare});
                                                   return $weak->{connect_timeout} || 0.5;
                                               },
                                               peername => $self->{server});
}


sub disconnect {
    my ($self) = @_;

    DEBUG "C [%s]: disconnecting", $self->{server};
    if (my $handle = $self->{handle}) {
        DEBUG "C [%s]: got handle", $self->{server};
        eval {
            $handle->stop_read;
            $handle->push_shutdown();
            $handle->destroy();
        };
    }

    DEBUG "C [%s]: failing all requests", $self->{server};
    $self->fail;
}


sub enqueue {
    my ($self, $request, $failback) = @_;
    $self->connect unless ($self->{handle});
    DEBUG "C [%s]: queuing request", $self->{server};
    $self->{last} = AE::time;
    push @{$self->{queue}}, {request => $request, failback => $failback};
    $self->dequeue;
    return 1;
}


sub dequeue {
    my ($self) = @_;
    my $weak = $self; weaken ($weak);
    return if ($self->{executing});
    DEBUG "C [%s]: checking for job", $self->{server};
    return unless ($self->{executing} = shift @{$self->{queue}});
    DEBUG "C [%s]: executing", $self->{server};
    $self->{executing}->{request}->($self->{handle},
                                    sub {
                                        $weak->{requests}++;
                                        DEBUG "C [%s]: done with request", $weak->{server};
                                        delete $weak->{executing};
                                        $weak->dequeue;
                                    },
                                    $self->{server});
}


sub fail {
    my ($self) = @_;
    DEBUG "C [%s]: failing requests", $self->{server};
    my @requests;

    if (my $request = delete $self->{executing}) {
        push @requests, $request;
    }
    while (my $request = shift @{$self->{queue}}) {
        push @requests, $request;
    }

    for my $request (@requests) {
        DEBUG "C [%s]: failing request %s", $self->{server}, $request;
        $request->{failback}->();
        undef $request;
    }
}

1;

__END__
=pod

=head1 NAME

Memcached::Client::Connection - Class to manage Memcached::Client server connections

=head1 VERSION

version 1.07

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

=head2 disconnect

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

