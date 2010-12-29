package Memcached::Client::Connection;
# ABSTRACT: Class to manage Memcached::Client server connections

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Memcached::Client::Log qw{DEBUG INFO};
use Scalar::Util qw{weaken};

=head1 SYNOPSIS

  use Memcached::Client::Connection;
  my $connection = Memcached::Client::Connection->new ("server:port");
  $connection->enqueue ($request->($handle, $callback), $failback);

=method new

C<new()> builds a new connection object.

Its only parameter is the server specification, in the form of
"hostname:port".  The object is constructed and returns immediately.

=cut

sub new {
    my ($class, $server, $preparation) = @_;
    my $self = bless {attempts => 0, last => 0, prepare => $preparation, requests => 0, queue => [], server => $server}, $class;
    return $self;
}

=method connect

C<connect()> initiates a connection to the specified server.

If it succeeds, it will start dequeuing requests for the server to
satisfy.

If it fails, it will respond to all outstanding requests by invoking
their failback routine.

=cut

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
                                                   $weak->{prepare}->($handle);
                                                   return $weak->{connect_timeout} || 0.5;
                                               },
                                               peername => $self->{server});
}

=method enqueue

C<enqueue()> adds the request specified (request, failback) pair to
the queue of requests to be processed.

=cut

sub enqueue {
    my ($self, $request, $failback) = @_;
    $self->connect unless ($self->{handle});
    DEBUG "C [%s]: queuing request", $self->{server};
    $self->{last} = AE::time;
    push @{$self->{queue}}, {request => $request, failback => $failback};
    $self->dequeue;
    return 1;
}

=method dequeue

C<dequeue()> manages the process of pulling requests off the queue and
executing them as possible.  Each request is handed the handle for the
server connection as well as a callback that will mark it as done and
do the next request.

If the request code fails to invoke this callback, processing will
halt.

=cut

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

=method fail

C<fail()> is called when there is an error on the handle, and it
invokes the failbacks of all queued requests.

=cut

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
