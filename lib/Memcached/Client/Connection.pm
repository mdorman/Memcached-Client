package Memcached::Client::Connection;
# ABSTRACT: Class to manage Memcached::Client server connections

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Memcached::Client::Log qw{DEBUG INFO};

=head1 SYNOPSIS

  use Memcached::Client::Connection;
  my $connection = Memcached::Client::Connection->new ("server:port");
  $connection->enqueue ($request);

=head1 DESCRIPTION

A C<Memcached::Client::Connection> object is responsible for managing
a connection to a particular memcached server, and a queue of requests
destined for that server.

Connections are, by default, made lazily.

The connection handler will try to automatically reconnect several
times on connection failure, only returning failure responses for all
queued requests as a last resort.

=method new

C<new()> builds a new connection object.  The object is constructed
and returned immediately.

Takes two parameters: one is the server specification, in the form of
"hostname" or "hostname:port".  If no port is specified, ":11211" (the
default memcached port) is appended to the server name.

The other, optional, parameter is a subroutine reference that will be
invoked on the raw filehandle before connection.  Generally only
useful for putting the filehandle in binary mode.

No connection is initiated at construction time, because that would
require that we perhaps accept a callback to signal completion, etc.
Simpler to lazily construct the connection when the conditions are
already right for doing our asynchronous dance.

=cut

sub new {
    my ($class, $server, $preparation) = @_;
    die "You must give me a server to connect to.\n" unless ($server);
    $server .= ":11211" unless index $server, ':' > 0;
    my $self = bless {prepare => $preparation, queue => [], server => $server}, $class;
    return $self;
}

=method connect

C<connect()> initiates a connection to the specified server.

It takes an optional callback parameter that is used to signal when it
has either completed the connection, or given up in despair.

If it succeeds, it will start processing requests for the server to
satisfy.

If it fails, it will respond to all outstanding requests by invoking
their failback routine.

=cut

sub connect {
    my ($self, $callback) = @_;
    if ($self->{handle}) {
        DEBUG "Already connected to %s", $self->{server};
        $callback->();
    } else {
        DEBUG "Initiating connection to %s", $self->{server};
        $self->{callback} = $callback;
        my $attempts = 0;
        $self->{handle} = AnyEvent::Handle->new (connect => [split /:/, $self->{server}],
                                                 keepalive => 1,
                                                 on_connect => sub {
                                                     DEBUG "Connected with %s", $self->{server};
                                                     $self->callback;
                                                     $self->{executing}->run ($self) if ($self->{executing});
                                                 },
                                                 on_error => sub {
                                                     my ($handle, $fatal, $message) = @_;
                                                     DEBUG "Removing handle for %s", $self->{server};
                                                     delete $self->{handle};
                                                     if ($message eq "Broken pipe") {
                                                         DEBUG "Broken pipe, reconnecting to %s", $self->{server};
                                                         $self->connect;
                                                     } elsif ($message eq "Connection timed out" and ++$attempts < 5) {
                                                         DEBUG "Connection timed out, retrying to %s", $self->{server};
                                                         $self->connect;
                                                     } else {
                                                         DEBUG "Error %s, failing %s", $message, $self->{server};
                                                         $self->callback;
                                                         $self->fail;
                                                     }
                                                 },
                                                 on_prepare => sub {
                                                     my ($handle) = @_;
                                                     DEBUG "Preparing handle for %s", $self->{server};
                                                     $self->{prepare}->($handle) if ($self->{prepare});
                                                     return $self->{connect_timeout} || 0.5;
                                                 });
    }
}

=method callback

=cut

sub callback {
    my ($self) = @_;
    if (my $cb = delete $self->{callback}) {
        $cb->();
    }
}

=method disconnect

=cut

sub disconnect {
    my ($self) = @_;

    DEBUG "disconnecting";
    if (my $handle = $self->{handle}) {
        DEBUG "got handle";
        eval {
            $handle->stop_read;
            $handle->push_shutdown();
            $handle->destroy();
        };
    }

    DEBUG "failing all requests";
    $self->fail;
}

=method enqueue

C<enqueue()> adds the specified request object to the queue of
requests to be processed, if there is already a request in progress,
otherwise, it begins execution of the specified request.  If
necessary, it will initiate connection to the server as well.

=cut

sub enqueue {
    my ($self, $request) = @_;
    if ($self->{executing}) {
        DEBUG 'queueing request';
        push @{$self->{queue}}, $request;
    } else {
        $self->{executing} = $request;
        DEBUG "Executing";
        if ($self->{handle}) {
            $request->run ($self);
        } else {
            $self->connect;
        }
    }
}

=method complete

=cut

sub complete {
    my ($self) = @_;
    DEBUG "Done with request";
    $self->{executing}->complete if ($self->{executing});
    if ($self->{executing} = shift @{$self->{queue}}) {
        DEBUG "Executing";
        $self->{executing}->run ($self);
    }
}

=method fail

C<fail()> is called when there is an error on the handle, and it
invokes the failbacks of all queued requests.

=cut

sub fail {
    my ($self) = @_;
    DEBUG "Failing requests";
    for my $request (grep {defined} delete $self->{executing}, @{$self->{queue}}) {
        DEBUG "Failing request %s", $request;
        $request->complete;
        undef $request;
    }
}

1;
