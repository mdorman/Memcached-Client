package Memcached::Client::Connection;
# ABSTRACT: Class to manage Memcached::Client server connections

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Memcached::Client::Log qw{DEBUG INFO};
use Scalar::Util qw{refaddr weaken};

=head1 SYNOPSIS

  use Memcached::Client::Connection;
  my $connection = Memcached::Client::Connection->new ("server:port");
  $connection->enqueue ($request->($handle, $callback), $failback);

=method new

C<new()> builds a new connection object.

Its only parameter is the server specification, in the form of
"hostname:port".  The object is constructed and returns immediately.

=cut

my $id = 0;

sub new {
    my ($class, $server, $preparation) = @_;
    my $self = bless {attempts => 0, id => $id++, last => 0, prepare => $preparation, requests => 0, queue => [], server => $server}, $class;
    $self->debug ("Constructed");
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

    $self->debug ("connecting to [%s]", $self->{server});

    $callback->() if $self->{handle};
    $self->{handle} ||= AnyEvent::Handle->new (connect => [$host, $port],
                                               keepalive => 1,
                                               on_connect => sub {
                                                   local *__ANON__ = "Memcached::Client::Connection::on_connect";
                                                   my ($handle, $host, $port) = @_;
                                                   $self->debug ("connected");
                                                   $weak->{attempts} = 0;
                                                   $weak->{last} = 0;
                                                   $weak->{requests} = 0;
                                                   $callback->() if ($callback);
                                                   $weak->dequeue;
                                               },
                                               on_error => sub {
                                                   local *__ANON__ = "Memcached::Client::Connection::on_error";
                                                   my ($handle, $fatal, $message) = @_;
                                                   my $last = $weak->{last} ? AE::time - $weak->{last} : 0;
                                                   my $pending = scalar @{$weak->{queue}};
                                                   # Need this here in case connection fails
                                                   if ($message eq "Broken pipe") {
                                                       $self->debug ("reconnecting broken pipe");
                                                       delete $weak->{handle};
                                                       $weak->connect;
                                                   } elsif ($message eq "Connection timed out" and ++$weak->{attempts} < 5) {
                                                       $self->debug ("reconnecting timeout");
                                                       delete $weak->{handle};
                                                       $weak->connect ($callback);
                                                   } else {
                                                       INFO "%d: %s, %d attempts, %d completed, %d pending, %f last", $self->{id}, $message, $weak->{attempts}, $weak->{requests}, $pending, $last;
                                                       $callback->() if ($callback);
                                                       delete $weak->{handle};
                                                       $weak->fail;
                                                   }
                                               },
                                               on_prepare => sub {
                                                   local *__ANON__ = "Memcached::Client::Connection::on_prepare";
                                                   my ($handle) = @_;
                                                   $self->debug ("preparing handle");
                                                   $weak->{prepare}->($handle) if ($weak->{prepare});
                                                   return $weak->{connect_timeout} || 0.5;
                                               });
}

=method debug

=cut

sub debug {
    my $self = shift;
    my $message = '%d: ' . shift;
    unshift @_, $message, $self->{id};
    goto &DEBUG;
}

=method disconnect

=cut

sub disconnect {
    my ($self) = @_;

    $self->debug ("disconnecting");
    if (my $handle = $self->{handle}) {
        $self->debug ("got handle");
        eval {
            $handle->stop_read;
            $handle->push_shutdown();
            $handle->destroy();
        };
    }

    $self->debug ("failing all requests");
    $self->fail;
}

=method enqueue

C<enqueue()> adds the request specified (request, failback) pair to
the queue of requests to be processed.

=cut

sub enqueue {
    my ($self, $request) = @_;
    $self->connect unless ($self->{handle});
    $self->debug ('queueing request');
    $self->{last} = AE::time;
    push @{$self->{queue}}, $request;
    $self->dequeue;
    return 1;
}

=method complete

=cut

sub complete {
    my ($self) = @_;
    $self->{requests}++;
    $self->debug ("Done with request");
    my $finished = delete $self->{executing};
    $finished->complete;
    $self->dequeue;
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
    $self->debug ("Checking for job");
    return unless ($self->{executing} = shift @{$self->{queue}});
    $self->debug ("Executing");
    $self->{executing}->run ($self);
}

=method fail

C<fail()> is called when there is an error on the handle, and it
invokes the failbacks of all queued requests.

=cut

sub fail {
    my ($self) = @_;
    $self->debug ("Failing requests");
    my @requests;

    if (my $request = delete $self->{executing}) {
        push @requests, $request;
    }
    while (my $request = shift @{$self->{queue}}) {
        push @requests, $request;
    }

    for my $request (@requests) {
        $self->debug ("Failing request %s", $request);
        $request->complete;
        undef $request;
    }
}

1;
